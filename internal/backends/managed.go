package backends

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
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
	managedStartupTimeout      = 45 * time.Second
	managedCleanupTimeout      = 20 * time.Second
	managedAPIProbeTimeout     = 20 * time.Second
	managedAPIProbeMinBackoff  = 100 * time.Millisecond
	managedAPIProbeMaxBackoff  = 1 * time.Second
)

type managedContainer interface {
	Endpoint(ctx context.Context, proto string) (string, error)
	Terminate(ctx context.Context, opts ...testcontainers.TerminateOption) error
}

type managedContainerStarter func(ctx context.Context, req testcontainers.GenericContainerRequest) (managedContainer, error)

type ManagedManager struct {
	instances int
	image     string
	stateDir  string

	mu         sync.Mutex
	containers []managedContainer

	startContainer managedContainerStarter
	probeHostPort  func(ctx context.Context, hostport string) error
	probeAPI       func(ctx context.Context, endpoint string) error
}

func NewManagedManager(instances int, image string, stateDir string) *ManagedManager {
	return &ManagedManager{
		instances:      instances,
		image:          image,
		stateDir:       stateDir,
		startContainer: defaultManagedContainerStarter,
		probeHostPort:  probeHostPort,
		probeAPI:       probeManagedAPI,
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
	if image == "" {
		return nil, fmt.Errorf("managed mode requires a non-empty image")
	}

	m.mu.Lock()
	if len(m.containers) > 0 {
		m.mu.Unlock()
		return nil, fmt.Errorf("managed manager already started")
	}
	m.mu.Unlock()

	starter := m.startContainer
	if starter == nil {
		starter = defaultManagedContainerStarter
	}

	probe := m.probeHostPort
	if probe == nil {
		probe = probeHostPort
	}
	probeAPI := m.probeAPI
	if probeAPI == nil {
		probeAPI = probeManagedAPI
	}

	startedContainers := make([]managedContainer, 0, m.instances)
	backends := make([]Backend, 0, m.instances)

	for i := 0; i < m.instances; i++ {
		request, err := m.containerRequestForInstance(i, image)
		if err != nil {
			cleanupErr := terminateManagedContainers(startedContainers)
			if cleanupErr != nil {
				return nil, errors.Join(err, cleanupErr)
			}
			return nil, err
		}

		container, err := starter(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: request,
			Started:          true,
		})
		if err != nil {
			cleanupErr := terminateManagedContainers(startedContainers)
			startErr := fmt.Errorf("start managed backend %d: %w", i, err)
			if cleanupErr != nil {
				return nil, errors.Join(startErr, cleanupErr)
			}
			return nil, startErr
		}
		startedContainers = append(startedContainers, container)

		endpoint, err := container.Endpoint(ctx, "http")
		if err != nil {
			cleanupErr := terminateManagedContainers(startedContainers)
			resolveErr := fmt.Errorf("resolve managed backend %d endpoint: %w", i, err)
			if cleanupErr != nil {
				return nil, errors.Join(resolveErr, cleanupErr)
			}
			return nil, resolveErr
		}
		endpoint = strings.TrimRight(endpoint, "/")

		parsed, err := url.Parse(endpoint)
		if err != nil {
			cleanupErr := terminateManagedContainers(startedContainers)
			parseErr := fmt.Errorf("parse managed backend %d endpoint %q: %w", i, endpoint, err)
			if cleanupErr != nil {
				return nil, errors.Join(parseErr, cleanupErr)
			}
			return nil, parseErr
		}
		if parsed.Host == "" {
			cleanupErr := terminateManagedContainers(startedContainers)
			parseErr := fmt.Errorf("managed backend %d endpoint %q is missing host", i, endpoint)
			if cleanupErr != nil {
				return nil, errors.Join(parseErr, cleanupErr)
			}
			return nil, parseErr
		}

		if err := probe(ctx, hostPort(parsed)); err != nil {
			cleanupErr := terminateManagedContainers(startedContainers)
			probeErr := fmt.Errorf("probe managed backend %d endpoint %q: %w", i, endpoint, err)
			if cleanupErr != nil {
				return nil, errors.Join(probeErr, cleanupErr)
			}
			return nil, probeErr
		}
		if err := probeAPI(ctx, endpoint); err != nil {
			cleanupErr := terminateManagedContainers(startedContainers)
			probeErr := fmt.Errorf("probe managed backend %d API at %q: %w", i, endpoint, err)
			if cleanupErr != nil {
				return nil, errors.Join(probeErr, cleanupErr)
			}
			return nil, probeErr
		}

		backends = append(backends, Backend{
			ID:       i,
			Endpoint: endpoint,
		})
	}

	m.mu.Lock()
	if len(m.containers) > 0 {
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

func (m *ManagedManager) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	m.mu.Lock()
	containers := append([]managedContainer(nil), m.containers...)
	m.containers = nil
	m.mu.Unlock()

	var errs []error
	for i := len(containers) - 1; i >= 0; i-- {
		if err := containers[i].Terminate(ctx); err != nil {
			errs = append(errs, fmt.Errorf("terminate managed backend %d: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

func (m *ManagedManager) containerRequestForInstance(instanceID int, image string) (testcontainers.ContainerRequest, error) {
	request := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{managedDynamoPort},
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
	return request, nil
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
