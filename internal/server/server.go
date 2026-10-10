package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"RaftKV/internal/raft"
)

type KVServer struct {
	node *raft.RaftNode //This is the Raft node that actually handles consensus.
	mux  *http.ServeMux //This is Go's HTTP router. It decides which handler receives an incoming HTTP request.
	//HTTP Client -> KVServer -> RaftNode
}

type PutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Response struct {
	Success  bool   `json:"success,omitempty"`
	Value    string `json:"value"`
	Found    bool   `json:"found"`
	Error    string `json:"error,omitempty"`
}

func NewKVServer(node *raft.RaftNode) *KVServer { //This creates the HTTP server around an existing Raft node.
	s := &KVServer{
		node: node,
		mux:  http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}

func (s *KVServer) registerRoutes() {
	s.mux.HandleFunc("/kv", s.handleKVRequest) //Whenever an HTTP request comes to /kv, call handleKVRequest.
}

func (s *KVServer) handleKVRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodPost:
		var req PutRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		success, err := s.node.Put(req.Key, req.Value)
		if errors.Is(err, raft.ErrWrongLeader) {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(Response{Error: "not the leader"})
			return
		} else if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(Response{Error: err.Error()})
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(Response{Success: success})

	case http.MethodGet:
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key parameter", http.StatusBadRequest)
			return
		}

		val, found, err := s.node.Get(key)
		if errors.Is(err, raft.ErrWrongLeader) {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(Response{Error: "not the leader"})
			return
		} else if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(Response{Error: err.Error()})
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(Response{Value: val, Found: found})

	case http.MethodDelete:
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "missing key parameter", http.StatusBadRequest)
			return
		}

		success, err := s.node.Delete(key)
		if errors.Is(err, raft.ErrWrongLeader) {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(Response{Error: "not the leader"})
			return
		} else if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(Response{Error: err.Error()})
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(Response{Success: success})

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *KVServer) ServeHTTP(w http.ResponseWriter, r *http.Request) { //It means KVServer itself implements the http.Handler interface.
	s.mux.ServeHTTP(w, r)
}

// 200 = OK
// 400 = Bad Request
// 401 = Unauthorized
// 403 = Forbidden
// 404 = Not Found
// 405 = Method Not Allowed
// 500 = Internal Server Error
// 503 = Service Unavailable