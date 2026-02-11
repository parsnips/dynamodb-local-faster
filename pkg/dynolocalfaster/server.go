package dynolocalfaster

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/parsnips/dynamodb-local-faster/internal/backends"
	"github.com/parsnips/dynamodb-local-faster/internal/catalog"
	"github.com/parsnips/dynamodb-local-faster/internal/httpapi"
	"github.com/parsnips/dynamodb-local-faster/internal/httpx"
	"github.com/parsnips/dynamodb-local-faster/internal/metrics"
	"github.com/parsnips/dynamodb-local-faster/internal/partiql"
	"github.com/parsnips/dynamodb-local-faster/internal/router"
	"github.com/parsnips/dynamodb-local-faster/internal/streams"
)

type Server struct {
	cfg     Config
	manager backends.Manager
	state   *metrics.State

	mu sync.Mutex

	started  bool
	endpoint string

	apiServer     *http.Server
	apiListener   net.Listener
	metricsServer *http.Server
	metricsListen net.Listener
}

func New(ctx context.Context, cfg Config) (*Server, error) {
	_ = ctx

	normalized, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}

	var manager backends.Manager
	switch normalized.Mode {
	case ModeAttached:
		manager = backends.NewAttachedManager(normalized.BackendEndpoints)
	case ModeManaged:
		manager = backends.NewManagedManager(
			normalized.Instances,
			normalized.DynamoImage,
			normalized.StateDir,
		)
	default:
		return nil, fmt.Errorf("unsupported mode %q", normalized.Mode)
	}

	return &Server{
		cfg:      normalized,
		manager:  manager,
		state:    metrics.NewState(),
		endpoint: "http://" + normalized.ListenAddr,
	}, nil
}

func (s *Server) Start(ctx context.Context) (err error) {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("server already started")
	}
	s.started = true
	s.mu.Unlock()

	managerStarted := false
	startupCommitted := false
	defer func() {
		if startupCommitted {
			return
		}

		s.state.SetReady(false)
		s.setStarted(false)

		var cleanupErrs []error
		if s.metricsServer != nil {
			if shutdownErr := s.metricsServer.Shutdown(context.Background()); shutdownErr != nil && !errors.Is(shutdownErr, http.ErrServerClosed) {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("startup cleanup: shutdown metrics server: %w", shutdownErr))
			}
			s.metricsServer = nil
			s.metricsListen = nil
		}
		if s.apiServer != nil {
			if shutdownErr := s.apiServer.Shutdown(context.Background()); shutdownErr != nil && !errors.Is(shutdownErr, http.ErrServerClosed) {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("startup cleanup: shutdown api server: %w", shutdownErr))
			}
			s.apiServer = nil
			s.apiListener = nil
		}
		if managerStarted {
			if closeErr := s.manager.Close(context.Background()); closeErr != nil {
				cleanupErrs = append(cleanupErrs, fmt.Errorf("startup cleanup: close backend manager: %w", closeErr))
			}
		}
		if cleanupErr := errors.Join(cleanupErrs...); cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()

	backendsList, err := s.manager.Start(ctx)
	if err != nil {
		return err
	}
	managerStarted = true

	backendRouter, err := router.NewStaticRouter(backendsList)
	if err != nil {
		return err
	}

	httpClient := httpx.NewPooledClient(30 * time.Second)
	streamMux := streams.NewMux(backendsList, streams.MakeProxyFunc(httpClient))

	apiMux := http.NewServeMux()
	apiMux.Handle("/", httpapi.NewHandler(
		backendRouter,
		catalog.NewNoopReplicator(),
		streamMux,
		partiql.NewNoopParser(),
	))
	apiMux.HandleFunc("/healthz", s.state.HealthHandler)
	apiMux.HandleFunc("/readyz", s.state.ReadyHandler)
	if s.cfg.MetricsAddr == s.cfg.ListenAddr || s.cfg.MetricsAddr == "" {
		apiMux.HandleFunc("/metrics", s.state.MetricsHandler)
	}

	apiListener, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen on %q: %w", s.cfg.ListenAddr, err)
	}

	s.apiServer = &http.Server{Handler: apiMux}
	s.apiListener = apiListener
	s.endpoint = "http://" + apiListener.Addr().String()

	go s.serve("api", s.apiServer, apiListener)

	if s.cfg.MetricsAddr != "" && s.cfg.MetricsAddr != s.cfg.ListenAddr {
		metricsMux := http.NewServeMux()
		metricsMux.HandleFunc("/metrics", s.state.MetricsHandler)
		metricsMux.HandleFunc("/healthz", s.state.HealthHandler)
		metricsMux.HandleFunc("/readyz", s.state.ReadyHandler)

		metricsListener, metricsErr := net.Listen("tcp", s.cfg.MetricsAddr)
		if metricsErr != nil {
			return fmt.Errorf("listen on metrics addr %q: %w", s.cfg.MetricsAddr, metricsErr)
		}

		s.metricsServer = &http.Server{Handler: metricsMux}
		s.metricsListen = metricsListener
		go s.serve("metrics", s.metricsServer, metricsListener)
	}

	s.state.SetReady(true)
	startupCommitted = true
	return nil
}

func (s *Server) Endpoint() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.endpoint
}

func (s *Server) Close(ctx context.Context) error {
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return nil
	}

	apiServer := s.apiServer
	metricsServer := s.metricsServer
	manager := s.manager
	s.started = false
	s.mu.Unlock()

	s.state.SetReady(false)

	var errs []error
	if metricsServer != nil {
		if err := metricsServer.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs = append(errs, fmt.Errorf("shutdown metrics server: %w", err))
		}
	}
	if apiServer != nil {
		if err := apiServer.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs = append(errs, fmt.Errorf("shutdown api server: %w", err))
		}
	}
	if manager != nil {
		if err := manager.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("shutdown backend manager: %w", err))
		}
	}
	return errors.Join(errs...)
}

func (s *Server) setStarted(started bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.started = started
}

func (s *Server) serve(name string, srv *http.Server, listener net.Listener) {
	if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("%s server stopped with error: %v", name, err)
	}
}
