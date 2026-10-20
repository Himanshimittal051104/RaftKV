package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"RaftKV/internal/raft"
)

type KVServer struct {
	node *raft.RaftNode //This is the Raft node that actually handles consensus.
	mux  *http.ServeMux //This is Go's HTTP multiplexer or router. It decides which handler receives an incoming HTTP request.It maps URL paths to handler functions.
	//HTTP Client -> KVServer -> RaftNode
}

type WriteRequest struct {
	ClientID  int64  `json:"clientId"`
	SeqNumber int64  `json:"seqNumber"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

type Response struct {
	Success bool   `json:"success,omitempty"`
	Value   string `json:"value,omitempty"`
	Found   bool   `json:"found,omitempty"`
	Error   string `json:"error,omitempty"`
	//The omitempty tag omits a field when it has its zero value
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
	//w is the response writer used to send the response to the client. r contains the incoming HTTP request.

	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodPost:
		var req WriteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if req.ClientID == 0 || req.SeqNumber <= 0 {
			http.Error(w, "invalid clientId or seqNumber", http.StatusBadRequest)
			return
		}

		success, err := s.node.Put(req.ClientID, req.SeqNumber, req.Key, req.Value)
		if errors.Is(err, raft.ErrWrongLeader) {
			w.WriteHeader(http.StatusServiceUnavailable) //This sends the HTTP status code 503 Service Unavailable to the client.
			json.NewEncoder(w).Encode(Response{Error: "not the leader"})
			//json.NewEncoder(w) creates a JSON encoder that writes to the HTTP response writer w.
			//.Encode(...) converts the Response struct into JSON and writes it to the response.
			return
		} else if errors.Is(err, raft.ErrRequestIDConflict) {
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(Response{Error: "request ID reused with a different command"})
			return
		} else if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(Response{Error: err.Error()})
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(Response{Success: success})

	case http.MethodGet:
		var req WriteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if req.ClientID == 0 || req.SeqNumber <= 0 || req.Key == "" {
			http.Error(w, "invalid clientId, seqNumber, or key", http.StatusBadRequest)
			return
		}

		val, found, err := s.node.Get(req.ClientID, req.SeqNumber, req.Key)
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
		var req WriteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if req.ClientID == 0 || req.SeqNumber <= 0 || req.Key == "" {
			http.Error(w, "invalid clientId, seqNumber, or key", http.StatusBadRequest)
			return
		}

		success, err := s.node.Delete(req.ClientID, req.SeqNumber, req.Key)

		if errors.Is(err, raft.ErrWrongLeader) {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(Response{Error: "not the leader"})
			return
		} else if errors.Is(err, raft.ErrRequestIDConflict) {
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(Response{Error: "request ID reused with a different command"})
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
	//It means KVServer itself implements the http.Handler interface.
	s.mux.ServeHTTP(w, r)
}

// 200 = OK
// 400 = Bad Request
// 500 = Internal Server Error
// 503 = Service Unavailable
// 409 =Conflict
