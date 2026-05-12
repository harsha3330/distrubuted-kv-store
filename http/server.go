package httpd

import (
	"encoding/json"
	"net/http"

	st "github.com/harsha3330/distubuted-kv-store/store"
)

type store interface {
	Get(key string) (string, error)
	Set(key, value string) error
	Delete(key string) error
}

type Server struct {
	Store    store
	httpAddr string
	mux      *http.ServeMux
}

func NewServer(httpAddr string) *Server {
	s := &Server{
		Store:    st.NewStore(),
		httpAddr: httpAddr,
		mux:      http.NewServeMux(),
	}

	s.routes()

	return s
}

func (srv *Server) routes() {
	srv.mux.HandleFunc("GET /key/{key}", srv.handleGet)
	srv.mux.HandleFunc("POST /key", srv.handleSet)
	srv.mux.HandleFunc("DELETE /key/{key}", srv.handleDelete)
	srv.mux.HandleFunc("GET /health", srv.handleHealth)
}

func (srv *Server) Start() error {
	server := http.Server{
		Addr:    srv.httpAddr,
		Handler: srv.mux,
	}

	return server.ListenAndServe()
}

func (srv *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ok",
	})
}

func (srv *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	val, err := srv.Store.Get(key)
	if err != nil {
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
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if body.Key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	if err := srv.Store.Set(body.Key, body.Value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (srv *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	if err := srv.Store.Delete(key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
