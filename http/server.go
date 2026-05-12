package httpd

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	st "github.com/harsha3330/distubuted-kv-store/store"
)

type store interface {
	Get(key string) (string, error)
	Set(key, value string) error
	Delete(key string) error
	Status() (st.StoreStatus, error)
	Join(nodeID, raftAddr string) error
}

type Server struct {
	Store    store
	httpAddr string
	mux      *http.ServeMux
	logger   *log.Logger
}

func NewServer(httpAddr string) *Server {
	s := &Server{
		Store:    st.NewStore(),
		httpAddr: httpAddr,
		mux:      http.NewServeMux(),
		logger:   log.New(os.Stdout, "[httpd] ", log.LstdFlags|log.Lmsgprefix),
	}

	s.routes()

	return s
}

func (srv *Server) routes() {
	srv.mux.HandleFunc("GET /key/{key}", srv.handleGet)
	srv.mux.HandleFunc("POST /key", srv.handleSet)
	srv.mux.HandleFunc("DELETE /key/{key}", srv.handleDelete)
	srv.mux.HandleFunc("GET /health", srv.handleHealth)
	srv.mux.HandleFunc("GET /status", srv.handleStatus)
	srv.mux.HandleFunc("POST /join", srv.handleJoin)
}

func (srv *Server) Start() error {
	server := http.Server{
		Addr:    srv.httpAddr,
		Handler: srv.requestLogger(srv.mux),
	}

	srv.logger.Printf("listening on %s", srv.httpAddr)
	return server.ListenAndServe()
}

// requestLogger logs every request with method, path, status, duration and remote addr.
func (srv *Server) requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(rec, r)

		srv.logger.Printf("%s %s -> %d (%s) from %s",
			r.Method, r.URL.Path, rec.status, time.Since(start), r.RemoteAddr)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (srv *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ok",
	})
}

func (srv *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	status, err := srv.Store.Status()
	if err != nil {
		srv.logger.Printf("status: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	json.NewEncoder(w).Encode(status)
}

func (srv *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	val, err := srv.Store.Get(key)
	if err != nil {
		srv.logger.Printf("get key=%q: %v", key, err)
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(map[string]string{
		"key":   key,
		"value": val,
	})
}

func (srv *Server) handleSet(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		srv.logger.Printf("set: decode body: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if body.Key == "" {
		srv.logger.Printf("set: missing key")
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	if err := srv.Store.Set(body.Key, body.Value); err != nil {
		srv.logger.Printf("set key=%q: %v", body.Key, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	srv.logger.Printf("set key=%q", body.Key)
	w.WriteHeader(http.StatusCreated)
}

func (srv *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	if err := srv.Store.Delete(key); err != nil {
		srv.logger.Printf("delete key=%q: %v", key, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	srv.logger.Printf("delete key=%q", key)
	w.WriteHeader(http.StatusOK)
}

func (srv *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	var body struct {
		NodeID   string `json:"nodeID"`
		RaftAddr string `json:"raftAddr"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		srv.logger.Printf("join: decode body: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if body.NodeID == "" || body.RaftAddr == "" {
		srv.logger.Printf("join: missing nodeID or raftAddr")
		http.Error(w, "nodeID and raftAddr are required", http.StatusBadRequest)
		return
	}

	if err := srv.Store.Join(body.NodeID, body.RaftAddr); err != nil {
		srv.logger.Printf("join nodeID=%q raftAddr=%q: %v", body.NodeID, body.RaftAddr, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	srv.logger.Printf("joined nodeID=%q raftAddr=%q", body.NodeID, body.RaftAddr)
	w.WriteHeader(http.StatusOK)
}
