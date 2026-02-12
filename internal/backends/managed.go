package backends

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	testcontainers "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	managedDynamoPort          = "8000/tcp"
	managedDynamoStateMountDir = "/home/dynamodblocal/data"
	managedDynamoJarName       = "DynamoDBLocal.jar"
	managedDynamoLibDirName    = "DynamoDBLocal_lib"
	managedDynamoPathEnv       = "DLF_DYNAMODB_LOCAL_PATH"
	managedStartupTimeout      = 45 * time.Second
	managedCleanupTimeout      = 20 * time.Second
	managedAPIProbeTimeout     = 20 * time.Second
	managedAPIProbeMinBackoff  = 100 * time.Millisecond
	managedAPIProbeMaxBackoff  = 1 * time.Second
)

const (
	ManagedRuntimeHost      = "host"
	ManagedRuntimeContainer = "container"
)

type managedContainer interface {
	Endpoint(ctx context.Context, proto string) (string, error)
	Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error
}

type managedContainerStarter func(ctx context.Context, req testcontainers.GenericContainerRequest) (managedContainer, error)

type managedHostProcess struct {
	endpoint string
	cmd      *exec.Cmd
	logFile  *os.File
}

type managedHostStarter func(ctx context.Context) ([]Backend, []*managedHostProcess, error)

type managedHostPrereqError struct {
	reason string
}

func (e *managedHostPrereqError) Error() string { return e.reason }

type ManagedManager struct {
	instances int
	image     string
	stateDir  string
	runtime   string

	dynamoLocalPath string

	mu         sync.Mutex
	containers []managedContainer
	hostProcs  []*managedHostProcess

	startContainer managedContainerStarter
	startHost      managedHostStarter
	lookPath       func(file string) (string, error)
	probeHostPort  func(ctx context.Context, hostport string) error
	probeAPI       func(ctx context.Context, endpoint string) error
	logf           func(format string, v ...any)
}

func NewManagedManager(instances int, image string, stateDir string) *ManagedManager {
	return NewManagedManagerWithRuntime(instances, image, stateDir, ManagedRuntimeHost, "")
}

func NewManagedManagerWithRuntime(
	instances int,
	image string,
	stateDir string,
	runtime string,
	dynamoLocalPath string,
) *ManagedManager {
	return &ManagedManager{
		instances:       instances,
		image:           image,
		stateDir:        stateDir,
		runtime:         runtime,
		dynamoLocalPath: dynamoLocalPath,
		startContainer:  defaultManagedContainerStarter,
		startHost:       nil,
		lookPath:        exec.LookPath,
		probeHostPort:   probeHostPort,
		probeAPI:        probeManagedAPI,
		logf:            log.Printf,
	}
}

func (m *ManagedManager) Start(ctx context.Context) ([]Backend, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if m.instances <= 0 {
		return nil, fmt.Errorf("managed mode requires instances > 0, got %d", m.instances)
	}

	image := strings.TrimSpace(m.image)
	runtime := strings.ToLower(strings.TrimSpace(m.runtime))
	if runtime == "" {
		runtime = ManagedRuntimeHost
	}

	m.mu.Lock()
	if len(m.containers) > 0 || len(m.hostProcs) > 0 {
		m.mu.Unlock()
		return nil, fmt.Errorf("managed manager already started")
	}
	m.mu.Unlock()

	probe := m.probeHostPort
	if probe == nil {
		probe = probeHostPort
	}
	probeAPI := m.probeAPI
	if probeAPI == nil {
		probeAPI = probeManagedAPI
	}

	startContainerAndStore := func() ([]Backend, error) {
		if image == "" {
			return nil, fmt.Errorf("managed mode requires a non-empty image for container runtime")
		}
		starter := m.startContainer
		if starter == nil {
			starter = defaultManagedContainerStarter
		}
		backends, startedContainers, err := m.startContainerBackends(ctx, image, starter, probe, probeAPI)
		if err != nil {
			return nil, err
		}
		m.mu.Lock()
		if len(m.containers) > 0 || len(m.hostProcs) > 0 {
			m.mu.Unlock()
			cleanupErr := terminateManagedContainers(startedContainers)
			startErr := fmt.Errorf("managed manager already started")
			if cleanupErr != nil {
				return nil, errors.Join(startErr, cleanupErr)
			}
			return nil, startErr
		}
		m.containers = startedContainers
		m.mu.Unlock()
		return append([]Backend(nil), backends...), nil
	}

	switch runtime {
	case ManagedRuntimeContainer:
		return startContainerAndStore()
	case ManagedRuntimeHost:
		hostStarter := m.startHost
		if hostStarter == nil {
			hostStarter = func(runCtx context.Context) ([]Backend, []*managedHostProcess, error) {
				return m.startHostBackends(runCtx, probe, probeAPI)
			}
		}
		backends, startedHostProcs, err := hostStarter(ctx)
		if err != nil {
			if !isManagedHostPrereqError(err) {
				return nil, err
			}
			if m.logf != nil {
				m.logf("managed host runtime unavailable (%v); falling back to container runtime", err)
			}
			fallbackBackends, fallbackErr := startContainerAndStore()
			if fallbackErr != nil {
				return nil, errors.Join(
					fmt.Errorf("managed host runtime unavailable: %w", err),
					fmt.Errorf("container fallback failed: %w", fallbackErr),
				)
			}
			return fallbackBackends, nil
		}
		m.mu.Lock()
		if len(m.containers) > 0 || len(m.hostProcs) > 0 {
			m.mu.Unlock()
			cleanupErr := terminateManagedHostProcesses(startedHostProcs)
			startErr := fmt.Errorf("managed manager already started")
			if cleanupErr != nil {
				return nil, errors.Join(startErr, cleanupErr)
			}
			return nil, startErr
		}
		m.hostProcs = startedHostProcs
		m.mu.Unlock()
		return append([]Backend(nil), backends...), nil
	default:
		return nil, fmt.Errorf(
			"unsupported managed backend runtime %q (want %q or %q)",
			runtime,
			ManagedRuntimeHost,
			ManagedRuntimeContainer,
		)
	}
}

func (m *ManagedManager) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	m.mu.Lock()
	containers := append([]managedContainer(nil), m.containers...)
	hostProcs := append([]*managedHostProcess(nil), m.hostProcs...)
	m.containers = nil
	m.hostProcs = nil
	m.mu.Unlock()

	var errs []error
	for i := len(containers) - 1; i >= 0; i-- {
		if err := containers[i].Terminate(ctx); err != nil {
			errs = append(errs, fmt.Errorf("terminate managed backend %d: %w", i, err))
		}
	}
	if err := terminateManagedHostProcessesWithContext(ctx, hostProcs); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (m *ManagedManager) startContainerBackends(
	ctx context.Context,
	image string,
	starter managedContainerStarter,
	probe func(context.Context, string) error,
	probeAPI func(context.Context, string) error,
) ([]Backend, []managedContainer, error) {
	startedContainers := make([]managedContainer, 0, m.instances)
	backends := make([]Backend, 0, m.instances)

	for i := 0; i < m.instances; i++ {
		request, err := m.containerRequestForInstance(i, image)
		if err != nil {
			cleanupErr := terminateManagedContainers(startedContainers)
			if cleanupErr != nil {
				return nil, nil, errors.Join(err, cleanupErr)
			}
			return nil, nil, err
		}

		container, err := starter(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: request,
			Started:          true,
		})
		if err != nil {
			cleanupErr := terminateManagedContainers(startedContainers)
			startErr := fmt.Errorf("start managed backend %d: %w", i, err)
			if cleanupErr != nil {
				return nil, nil, errors.Join(startErr, cleanupErr)
			}
			return nil, nil, startErr
		}
		startedContainers = append(startedContainers, container)

		endpoint, err := container.Endpoint(ctx, "http")
		if err != nil {
			cleanupErr := terminateManagedContainers(startedContainers)
			resolveErr := fmt.Errorf("resolve managed backend %d endpoint: %w", i, err)
			if cleanupErr != nil {
				return nil, nil, errors.Join(resolveErr, cleanupErr)
			}
			return nil, nil, resolveErr
		}
		endpoint = strings.TrimRight(endpoint, "/")

		parsed, err := url.Parse(endpoint)
		if err != nil {
			cleanupErr := terminateManagedContainers(startedContainers)
			parseErr := fmt.Errorf("parse managed backend %d endpoint %q: %w", i, endpoint, err)
			if cleanupErr != nil {
				return nil, nil, errors.Join(parseErr, cleanupErr)
			}
			return nil, nil, parseErr
		}
		if parsed.Host == "" {
			cleanupErr := terminateManagedContainers(startedContainers)
			parseErr := fmt.Errorf("managed backend %d endpoint %q is missing host", i, endpoint)
			if cleanupErr != nil {
				return nil, nil, errors.Join(parseErr, cleanupErr)
			}
			return nil, nil, parseErr
		}

		if err := probe(ctx, hostPort(parsed)); err != nil {
			cleanupErr := terminateManagedContainers(startedContainers)
			probeErr := fmt.Errorf("probe managed backend %d endpoint %q: %w", i, endpoint, err)
			if cleanupErr != nil {
				return nil, nil, errors.Join(probeErr, cleanupErr)
			}
			return nil, nil, probeErr
		}
		if err := probeAPI(ctx, endpoint); err != nil {
			cleanupErr := terminateManagedContainers(startedContainers)
			probeErr := fmt.Errorf("probe managed backend %d API at %q: %w", i, endpoint, err)
			if cleanupErr != nil {
				return nil, nil, errors.Join(probeErr, cleanupErr)
			}
			return nil, nil, probeErr
		}

		backends = append(backends, Backend{
			ID:       i,
			Endpoint: endpoint,
		})
	}

	return backends, startedContainers, nil
}

func (m *ManagedManager) containerRequestForInstance(instanceID int, image string) (testcontainers.ContainerRequest, error) {
	request := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{managedDynamoPort},
		Cmd:          managedDynamoCommandArgs("", false),
		WaitingFor: wait.ForExposedPort().
			WithStartupTimeout(managedStartupTimeout),
	}

	stateRoot := strings.TrimSpace(m.stateDir)
	if stateRoot == "" {
		return request, nil
	}

	instanceDir := filepath.Join(stateRoot, fmt.Sprintf("instance-%d", instanceID))
	absInstanceDir, err := filepath.Abs(instanceDir)
	if err != nil {
		return testcontainers.ContainerRequest{}, fmt.Errorf(
			"resolve state dir for managed backend %d: %w",
			instanceID,
			err,
		)
	}
	if err := os.MkdirAll(absInstanceDir, 0o755); err != nil {
		return testcontainers.ContainerRequest{}, fmt.Errorf(
			"create state dir for managed backend %d at %q: %w",
			instanceID,
			absInstanceDir,
			err,
		)
	}

	request.Mounts = testcontainers.Mounts(
		testcontainers.BindMount(absInstanceDir, testcontainers.ContainerMountTarget(managedDynamoStateMountDir)),
	)
	request.Cmd = managedDynamoCommandArgs(managedDynamoStateMountDir, true)
	return request, nil
}

func managedDynamoCommandArgs(statePath string, persistent bool) []string {
	args := []string{"-jar", managedDynamoJarName, "-sharedDb"}
	if persistent {
		return append(args, "-dbPath", statePath)
	}
	return append(args, "-inMemory")
}

func isManagedHostPrereqError(err error) bool {
	var prereqErr *managedHostPrereqError
	return errors.As(err, &prereqErr)
}

func (m *ManagedManager) startHostBackends(
	ctx context.Context,
	probe func(context.Context, string) error,
	probeAPI func(context.Context, string) error,
) ([]Backend, []*managedHostProcess, error) {
	lookup := m.lookPath
	if lookup == nil {
		lookup = exec.LookPath
	}
	javaBinary, err := lookup("java")
	if err != nil {
		return nil, nil, &managedHostPrereqError{reason: fmt.Sprintf("java not found: %v", err)}
	}

	jarPath, libDir, err := resolveManagedDynamoArtifacts(m.dynamoLocalPath)
	if err != nil {
		return nil, nil, err
	}

	startedProcs := make([]*managedHostProcess, 0, m.instances)
	backends := make([]Backend, 0, m.instances)
	for i := 0; i < m.instances; i++ {
		port, err := reserveManagedLoopbackPort()
		if err != nil {
			cleanupErr := terminateManagedHostProcesses(startedProcs)
			startErr := fmt.Errorf("reserve host backend %d port: %w", i, err)
			if cleanupErr != nil {
				return nil, nil, errors.Join(startErr, cleanupErr)
			}
			return nil, nil, startErr
		}
		endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)

		backendArgs := []string{
			"-Djava.library.path=" + libDir,
			"-jar", jarPath,
			"-sharedDb",
			"-port", strconv.Itoa(port),
		}

		stateRoot := strings.TrimSpace(m.stateDir)
		if stateRoot == "" {
			backendArgs = append(backendArgs, "-inMemory")
		} else {
			instanceDir := filepath.Join(stateRoot, fmt.Sprintf("instance-%d", i))
			absInstanceDir, err := filepath.Abs(instanceDir)
			if err != nil {
				cleanupErr := terminateManagedHostProcesses(startedProcs)
				startErr := fmt.Errorf("resolve state dir for managed host backend %d: %w", i, err)
				if cleanupErr != nil {
					return nil, nil, errors.Join(startErr, cleanupErr)
				}
				return nil, nil, startErr
			}
			if err := os.MkdirAll(absInstanceDir, 0o755); err != nil {
				cleanupErr := terminateManagedHostProcesses(startedProcs)
				startErr := fmt.Errorf(
					"create state dir for managed host backend %d at %q: %w",
					i,
					absInstanceDir,
					err,
				)
				if cleanupErr != nil {
					return nil, nil, errors.Join(startErr, cleanupErr)
				}
				return nil, nil, startErr
			}
			backendArgs = append(backendArgs, "-dbPath", absInstanceDir)
		}

		logFile, err := os.CreateTemp("", fmt.Sprintf("dlf-managed-host-backend-%d-*.log", i))
		if err != nil {
			cleanupErr := terminateManagedHostProcesses(startedProcs)
			startErr := fmt.Errorf("create log file for managed host backend %d: %w", i, err)
			if cleanupErr != nil {
				return nil, nil, errors.Join(startErr, cleanupErr)
			}
			return nil, nil, startErr
		}

		cmd := exec.Command(javaBinary, backendArgs...)
		cmd.Stdout = logFile
		cmd.Stderr = logFile
		cmd.Dir = filepath.Dir(jarPath)

		if err := cmd.Start(); err != nil {
			_ = logFile.Close()
			cleanupErr := terminateManagedHostProcesses(startedProcs)
			startErr := fmt.Errorf("start managed host backend %d: %w", i, err)
			if cleanupErr != nil {
				return nil, nil, errors.Join(startErr, cleanupErr)
			}
			return nil, nil, startErr
		}

		hostProc := &managedHostProcess{
			endpoint: endpoint,
			cmd:      cmd,
			logFile:  logFile,
		}
		startedProcs = append(startedProcs, hostProc)

		parsed, err := url.Parse(endpoint)
		if err != nil {
			cleanupErr := terminateManagedHostProcesses(startedProcs)
			parseErr := fmt.Errorf("parse managed host backend %d endpoint %q: %w", i, endpoint, err)
			if cleanupErr != nil {
				return nil, nil, errors.Join(parseErr, cleanupErr)
			}
			return nil, nil, parseErr
		}

		if err := waitForManagedHostPort(ctx, probe, hostPort(parsed)); err != nil {
			cleanupErr := terminateManagedHostProcesses(startedProcs)
			probeErr := fmt.Errorf("probe managed host backend %d endpoint %q: %w", i, endpoint, err)
			if cleanupErr != nil {
				return nil, nil, errors.Join(probeErr, cleanupErr)
			}
			return nil, nil, probeErr
		}
		if err := probeAPI(ctx, endpoint); err != nil {
			cleanupErr := terminateManagedHostProcesses(startedProcs)
			probeErr := fmt.Errorf("probe managed host backend %d API at %q: %w", i, endpoint, err)
			if cleanupErr != nil {
				return nil, nil, errors.Join(probeErr, cleanupErr)
			}
			return nil, nil, probeErr
		}

		backends = append(backends, Backend{
			ID:       i,
			Endpoint: endpoint,
		})
	}

	return backends, startedProcs, nil
}

func reserveManagedLoopbackPort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()

	tcpAddr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("unexpected listener addr type %T", listener.Addr())
	}
	return tcpAddr.Port, nil
}

func waitForManagedHostPort(
	ctx context.Context,
	probe func(context.Context, string) error,
	hostport string,
) error {
	probeCtx, cancel := context.WithTimeout(ctx, managedStartupTimeout)
	defer cancel()

	backoff := managedAPIProbeMinBackoff
	var lastErr error
	for {
		lastErr = probe(probeCtx, hostport)
		if lastErr == nil {
			return nil
		}

		timer := time.NewTimer(backoff)
		select {
		case <-probeCtx.Done():
			timer.Stop()
			return fmt.Errorf("wait for host port readiness: %w (last error: %v)", probeCtx.Err(), lastErr)
		case <-timer.C:
		}

		if backoff < managedAPIProbeMaxBackoff {
			backoff *= 2
			if backoff > managedAPIProbeMaxBackoff {
				backoff = managedAPIProbeMaxBackoff
			}
		}
	}
}

func resolveManagedDynamoArtifacts(configuredPath string) (string, string, error) {
	dedup := map[string]struct{}{}
	candidates := make([]string, 0, 8)
	addCandidate := func(path string) {
		path = strings.TrimSpace(path)
		if path == "" {
			return
		}
		if _, seen := dedup[path]; seen {
			return
		}
		dedup[path] = struct{}{}
		candidates = append(candidates, path)
	}

	if configuredPath != "" {
		jarPath, libDir, err := managedDynamoArtifactsFromPath(configuredPath)
		if err != nil {
			return "", "", &managedHostPrereqError{
				reason: fmt.Sprintf("invalid DynamoDB Local path %q: %v", configuredPath, err),
			}
		}
		return jarPath, libDir, nil
	}

	addCandidate(os.Getenv(managedDynamoPathEnv))
	if cwd, err := os.Getwd(); err == nil {
		addCandidate(cwd)
	}
	addCandidate("/tmp/dlf-dynamodb-local")
	if home, err := os.UserHomeDir(); err == nil {
		addCandidate(filepath.Join(home, ".dynamodb-local"))
	}
	addCandidate("/usr/local/share/dynamodb-local")
	addCandidate("/opt/dynamodb-local")

	for _, candidate := range candidates {
		jarPath, libDir, err := managedDynamoArtifactsFromPath(candidate)
		if err == nil {
			return jarPath, libDir, nil
		}
	}

	return "", "", &managedHostPrereqError{
		reason: fmt.Sprintf(
			"unable to locate DynamoDB Local artifacts; set %s or --dynamodb-local-path",
			managedDynamoPathEnv,
		),
	}
}

func managedDynamoArtifactsFromPath(path string) (string, string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", "", fmt.Errorf("path is empty")
	}

	info, err := os.Stat(path)
	if err != nil {
		return "", "", err
	}

	jarPath := path
	libDir := ""
	if info.IsDir() {
		jarPath = filepath.Join(path, managedDynamoJarName)
		libDir = filepath.Join(path, managedDynamoLibDirName)
	} else {
		if filepath.Base(path) != managedDynamoJarName {
			return "", "", fmt.Errorf("file path must point to %s", managedDynamoJarName)
		}
		libDir = filepath.Join(filepath.Dir(path), managedDynamoLibDirName)
	}

	jarInfo, err := os.Stat(jarPath)
	if err != nil {
		return "", "", fmt.Errorf("missing jar %q: %w", jarPath, err)
	}
	if jarInfo.IsDir() {
		return "", "", fmt.Errorf("jar path %q is a directory", jarPath)
	}

	libInfo, err := os.Stat(libDir)
	if err != nil {
		return "", "", fmt.Errorf("missing native libs dir %q: %w", libDir, err)
	}
	if !libInfo.IsDir() {
		return "", "", fmt.Errorf("native libs path %q is not a directory", libDir)
	}

	absJarPath, err := filepath.Abs(jarPath)
	if err != nil {
		return "", "", fmt.Errorf("resolve jar path %q: %w", jarPath, err)
	}
	absLibDir, err := filepath.Abs(libDir)
	if err != nil {
		return "", "", fmt.Errorf("resolve libs path %q: %w", libDir, err)
	}
	return absJarPath, absLibDir, nil
}

func terminateManagedHostProcesses(hostProcs []*managedHostProcess) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), managedCleanupTimeout)
	defer cancel()
	return terminateManagedHostProcessesWithContext(cleanupCtx, hostProcs)
}

func terminateManagedHostProcessesWithContext(ctx context.Context, hostProcs []*managedHostProcess) error {
	if len(hostProcs) == 0 {
		return nil
	}

	var errs []error
	for i := len(hostProcs) - 1; i >= 0; i-- {
		if err := terminateManagedHostProcess(ctx, hostProcs[i]); err != nil {
			errs = append(errs, fmt.Errorf("cleanup managed host backend %d: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

func terminateManagedHostProcess(ctx context.Context, hostProc *managedHostProcess) error {
	if hostProc == nil {
		return nil
	}

	var errs []error
	if hostProc.cmd != nil && hostProc.cmd.Process != nil {
		if err := hostProc.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			errs = append(errs, fmt.Errorf("kill process: %w", err))
		}

		waitDone := make(chan error, 1)
		go func() {
			waitDone <- hostProc.cmd.Wait()
		}()
		select {
		case waitErr := <-waitDone:
			if waitErr != nil {
				var exitErr *exec.ExitError
				if !errors.As(waitErr, &exitErr) {
					errs = append(errs, fmt.Errorf("wait process: %w", waitErr))
				}
			}
		case <-ctx.Done():
			errs = append(errs, fmt.Errorf("wait process: %w", ctx.Err()))
		}
	}

	if hostProc.logFile != nil {
		if err := hostProc.logFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close log file: %w", err))
		}
	}
	return errors.Join(errs...)
}

func defaultManagedContainerStarter(ctx context.Context, req testcontainers.GenericContainerRequest) (managedContainer, error) {
	return testcontainers.GenericContainer(ctx, req)
}

func probeManagedAPI(ctx context.Context, endpoint string) error {
	if ctx == nil {
		ctx = context.Background()
	}

	probeCtx, cancel := context.WithTimeout(ctx, managedAPIProbeTimeout)
	defer cancel()

	client, err := managedProbeClient(probeCtx, endpoint)
	if err != nil {
		return fmt.Errorf("build managed API probe client: %w", err)
	}

	backoff := managedAPIProbeMinBackoff
	var lastErr error

	for {
		lastErr = listTablesOnce(probeCtx, client)
		if lastErr == nil {
			return nil
		}

		timer := time.NewTimer(backoff)
		select {
		case <-probeCtx.Done():
			timer.Stop()
			return fmt.Errorf("wait for API readiness: %w (last error: %v)", probeCtx.Err(), lastErr)
		case <-timer.C:
		}

		if backoff < managedAPIProbeMaxBackoff {
			backoff *= 2
			if backoff > managedAPIProbeMaxBackoff {
				backoff = managedAPIProbeMaxBackoff
			}
		}
	}
}

func managedProbeClient(ctx context.Context, endpoint string) (*dynamodb.Client, error) {
	cfg, err := awscfg.LoadDefaultConfig(
		ctx,
		awscfg.WithRegion("us-west-2"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("local", "local", "")),
	)
	if err != nil {
		return nil, err
	}

	return dynamodb.NewFromConfig(cfg, func(options *dynamodb.Options) {
		options.BaseEndpoint = aws.String(endpoint)
	}), nil
}

func listTablesOnce(ctx context.Context, client *dynamodb.Client) error {
	_, err := client.ListTables(ctx, &dynamodb.ListTablesInput{
		Limit: aws.Int32(1),
	})
	return err
}

func terminateManagedContainers(containers []managedContainer) error {
	if len(containers) == 0 {
		return nil
	}

	cleanupCtx, cancel := context.WithTimeout(context.Background(), managedCleanupTimeout)
	defer cancel()

	var errs []error
	for i := len(containers) - 1; i >= 0; i-- {
		if err := containers[i].Terminate(cleanupCtx); err != nil {
			errs = append(errs, fmt.Errorf("cleanup managed backend %d: %w", i, err))
		}
	}
	return errors.Join(errs...)
}
