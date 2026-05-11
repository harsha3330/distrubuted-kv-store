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
	srv.mux.HandleFunc("/key", srv.handleKey)
	srv.mux.HandleFunc("/health", srv.handleHealth)
}

func (srv *Server) Start() error {
	server := http.Server{
		Addr:    srv.httpAddr,
		Handler: srv.mux,
	}

	return server.ListenAndServe()
}

func (srv *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("healthy"))
}

func (srv *Server) handleKey(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")

	switch r.Method {

	case http.MethodGet:
		val, err := srv.Store.Get(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		json.NewEncoder(w).Encode(map[string]string{
			"key":   key,
			"value": val,
		})

	case http.MethodPost:

		value := r.URL.Query().Get("value")

		err := srv.Store.Set(key, value)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)

	case http.MethodDelete:

		err := srv.Store.Delete(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
