package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"RaftKV/internal/raft"
)

type KVServer struct {
	node *raft.RaftNode
	mux  *http.ServeMux
}

type PutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Response struct {
	Success  bool   `json:"success,omitempty"`
	Value    string `json:"value,omitempty"`
	Found    bool   `json:"found,omitempty"`
	Error    string `json:"error,omitempty"`
}

func NewKVServer(node *raft.RaftNode) *KVServer {
	s := &KVServer{
		node: node,
		mux:  http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}

func (s *KVServer) registerRoutes() {
	s.mux.HandleFunc("/kv", s.handleKVRequest)
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

func (s *KVServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}