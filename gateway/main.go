package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "grpc-gateway/pb"
)

// ---------- request / response types ----------

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type registerRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type response struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
	Token   string `json:"token,omitempty"`
	MsgAddr string `json:"msg_addr,omitempty"`
	Error   string `json:"error,omitempty"`
}

type userInfoResponse struct {
	Success  bool   `json:"success"`
	UserID   int64  `json:"user_id,omitempty"`
	Username string `json:"username,omitempty"`
	Error    string `json:"error,omitempty"`
}

type validateResponse struct {
	Valid    bool   `json:"valid"`
	Username string `json:"username,omitempty"`
	UserID   int64  `json:"user_id,omitempty"`
}

type historyRequest struct {
	FriendID int64 `json:"friend_id"`
	Limit    int32 `json:"limit,omitempty"`
	Offset   int32 `json:"offset,omitempty"`
}

type historyResponse struct {
	Success  bool                `json:"success"`
	Messages []*pb.HistoryMessage `json:"messages"`
	Error    string              `json:"error,omitempty"`
}

type friendInfo struct {
	FriendID       int64  `json:"friend_id"`
	FriendUsername string `json:"friend_username"`
	Online         bool   `json:"online"`
	CreatedAt      string `json:"created_at"`
}

type friendsResponse struct {
	Success bool         `json:"success"`
	Friends []friendInfo `json:"friends,omitempty"`
	Error   string       `json:"error,omitempty"`
}

type pendingReqInfo struct {
	RequestID    int64  `json:"request_id"`
	FromUserID   int64  `json:"from_user_id"`
	FromUsername string `json:"from_username"`
	Status       int32  `json:"status"`
	CreatedAt    string `json:"created_at"`
}

type pendingReqsResponse struct {
	Success  bool             `json:"success"`
	Requests []pendingReqInfo `json:"requests,omitempty"`
	Error    string           `json:"error,omitempty"`
}

// ---------- globals ----------

var loginClient pb.LoginServiceClient
var authClient  pb.AuthServiceClient
var msgClient   pb.MsgServiceClient
var dbClient    pb.DatabaseServiceClient
var msgWSAddr   string

// ---------- helpers ----------

// mustDial connects to a gRPC server with insecure credentials.
// It is intended for controlled startup only; callers must arrange for cleanup.
func mustDial(target, name string) *grpc.ClientConn {
	conn, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to dial %s (%s): %v", name, target, err)
	}
	return conn
}

// authenticateRequest validates the Bearer token or cookie against the auth server.
// On success it returns the validated user identity and a cancelable context derived
// from the HTTP request (so disconnecting clients cancel gRPC calls automatically).
type authResult struct {
	UserID   int64
	Username string
	Exp      int64
	Context  context.Context
	Cancel   context.CancelFunc
}

func authenticateRequest(r *http.Request) (*authResult, error) {
	token := extractToken(r)
	if token == "" {
		return nil, fmt.Errorf("no token")
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	rep, err := authClient.ValidateToken(ctx, &pb.ValidateTokenRequest{Token: token})
	if err != nil || !rep.Valid {
		cancel()
		return nil, fmt.Errorf("invalid token")
	}
	return &authResult{
		UserID:   rep.UserId,
		Username: rep.Username,
		Exp:      rep.Exp,
		Context:  ctx,
		Cancel:   cancel,
	}, nil
}

func extractToken(r *http.Request) string {
	if auth := r.Header.Get("Authorization"); auth != "" {
		if strings.HasPrefix(auth, "Bearer ") {
			return strings.TrimPrefix(auth, "Bearer ")
		}
		return auth
	}
	if c, err := r.Cookie("token"); err == nil {
		return c.Value
	}
	return ""
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("writeJSON encode error: %v", err)
	}
}

func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next(w, r)
	}
}

// ---------- main ----------

func main() {
	addr := envDefault("GATEWAY_ADDR", ":8080")
	grpcAddr := envDefault("GRPC_ADDR", "127.0.0.1:50051")
	authAddr := envDefault("AUTH_GRPC_ADDR", "127.0.0.1:50053")
	dbAddr := envDefault("DB_GRPC_ADDR", "127.0.0.1:50052")
	msgGRPCAddr := envDefault("MSG_GRPC_ADDR", "127.0.0.1:50054")
	msgWSAddr = envDefault("MSG_WS_ADDR", "ws://127.0.0.1:50055/ws")

	// Establish gRPC connections
	conn := mustDial(grpcAddr, "login")
	defer conn.Close()
	loginClient = pb.NewLoginServiceClient(conn)

	authConn := mustDial(authAddr, "auth")
	defer authConn.Close()
	authClient = pb.NewAuthServiceClient(authConn)

	dbConn := mustDial(dbAddr, "db")
	defer dbConn.Close()
	dbClient = pb.NewDatabaseServiceClient(dbConn)

	msgConn := mustDial(msgGRPCAddr, "msg")
	defer msgConn.Close()
	msgClient = pb.NewMsgServiceClient(msgConn)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/login", corsMiddleware(handleLogin))
	mux.HandleFunc("/api/register", corsMiddleware(handleRegister))
	mux.HandleFunc("/api/logout", corsMiddleware(handleLogout))
	mux.HandleFunc("/api/user/info", corsMiddleware(handleUserInfo))
	mux.HandleFunc("/api/validate", corsMiddleware(handleValidate))
	mux.HandleFunc("/api/history", corsMiddleware(handleHistory))
	mux.HandleFunc("/api/friends", corsMiddleware(handleFriends))
	mux.HandleFunc("/api/pending-requests", corsMiddleware(handlePendingReqs))

	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	fmt.Printf("gRPC gateway listening on %s\n  login: %s  auth: %s  db: %s  msg: %s  ws: %s\n",
		addr, grpcAddr, authAddr, dbAddr, msgGRPCAddr, msgWSAddr)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ---------- handlers ----------

func handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeJSON(w, http.StatusMethodNotAllowed, response{Success: false, Error: "method not allowed"})
		return
	}
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, response{Success: false, Error: "invalid request body"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	stream, err := loginClient.Login(ctx)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, response{Success: false, Error: "gRPC stream failed"})
		return
	}
	if err := stream.Send(&pb.LoginRequest{Username: req.Username, Password: req.Password}); err != nil {
		writeJSON(w, http.StatusInternalServerError, response{Success: false, Error: "gRPC send failed"})
		return
	}
	stream.CloseSend()

	rep, err := stream.Recv()
	if err == io.EOF {
		writeJSON(w, http.StatusInternalServerError, response{Success: false, Error: "no response"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, response{Success: false, Error: "gRPC recv failed: " + err.Error()})
		return
	}

	if rep.Success {
		http.SetCookie(w, &http.Cookie{Name: "token", Value: rep.Token, Path: "/", HttpOnly: false})
		writeJSON(w, http.StatusOK, response{
			Success: true,
			Message: rep.Message,
			Token:   rep.Token,
			MsgAddr: msgWSAddr,
		})
	} else {
		writeJSON(w, http.StatusUnauthorized, response{Success: false, Message: rep.Message})
	}
}

func handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeJSON(w, http.StatusMethodNotAllowed, response{Success: false, Error: "method not allowed"})
		return
	}
	var req registerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, response{Success: false, Error: "invalid request body"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	rep, err := loginClient.Register(ctx, &pb.RegisterRequest{Username: req.Username, Password: req.Password})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, response{Success: false, Error: "gRPC call failed"})
		return
	}
	if rep.Success {
		writeJSON(w, http.StatusOK, response{Success: true, Message: rep.Message})
	} else {
		writeJSON(w, http.StatusConflict, response{Success: false, Message: rep.Message})
	}
}

func handleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeJSON(w, http.StatusMethodNotAllowed, response{Success: false, Error: "method not allowed"})
		return
	}
	auth, err := authenticateRequest(r)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, response{Success: false, Error: err.Error()})
		return
	}
	defer auth.Cancel()

	ttl := auth.Exp - time.Now().Unix()
	if ttl < 1 {
		ttl = 1
	}
	logoutCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	_, err = authClient.RevokeToken(logoutCtx, &pb.RevokeTokenRequest{UserId: auth.UserID, TtlSeconds: ttl})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, response{Success: false, Error: "revoke failed: " + err.Error()})
		return
	}

	http.SetCookie(w, &http.Cookie{Name: "token", Value: "", Expires: time.Unix(0, 0), Path: "/"})
	writeJSON(w, http.StatusOK, response{Success: true, Message: "logged out"})
}

func handleUserInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		writeJSON(w, http.StatusMethodNotAllowed, userInfoResponse{Success: false, Error: "method not allowed"})
		return
	}
	auth, err := authenticateRequest(r)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, userInfoResponse{Success: false, Error: err.Error()})
		return
	}
	defer auth.Cancel()

	writeJSON(w, http.StatusOK, userInfoResponse{Success: true, UserID: auth.UserID, Username: auth.Username})
}

func handleValidate(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		writeJSON(w, http.StatusMethodNotAllowed, validateResponse{Valid: false})
		return
	}
	auth, err := authenticateRequest(r)
	if err != nil {
		writeJSON(w, http.StatusOK, validateResponse{Valid: false})
		return
	}
	defer auth.Cancel()

	writeJSON(w, http.StatusOK, validateResponse{Valid: true, Username: auth.Username, UserID: auth.UserID})
}

func handleHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeJSON(w, http.StatusMethodNotAllowed, historyResponse{Success: false, Error: "method not allowed"})
		return
	}
	auth, err := authenticateRequest(r)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, historyResponse{Success: false, Error: err.Error()})
		return
	}
	defer auth.Cancel()

	var req historyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, historyResponse{Success: false, Error: "invalid request body"})
		return
	}
	if req.Limit <= 0 {
		req.Limit = 50
	}

	msgrep, err := msgClient.GetHistoryMessages(auth.Context, &pb.GetHistoryReq{
		UserId:     auth.UserID,
		WithUserId: req.FriendID,
		Limit:      req.Limit,
		Offset:     req.Offset,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, historyResponse{Success: false, Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, historyResponse{Success: true, Messages: msgrep.Messages})
}

func handleFriends(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		writeJSON(w, http.StatusMethodNotAllowed, friendsResponse{Success: false, Error: "method not allowed"})
		return
	}
	auth, err := authenticateRequest(r)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, friendsResponse{Success: false, Error: err.Error()})
		return
	}
	defer auth.Cancel()

	dbrep, err := dbClient.GetFriendships(auth.Context, &pb.GetFriendshipsReq{UserId: auth.UserID})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, friendsResponse{Success: false, Error: err.Error()})
		return
	}

	friends := make([]friendInfo, 0, len(dbrep.Friendships))
	for _, f := range dbrep.Friendships {
		friends = append(friends, friendInfo{
			FriendID:       f.FriendId,
			FriendUsername: f.FriendUsername,
			Online:         false,
			CreatedAt:      f.CreatedAt,
		})
	}

	writeJSON(w, http.StatusOK, friendsResponse{Success: true, Friends: friends})
}

func handlePendingReqs(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		writeJSON(w, http.StatusMethodNotAllowed, pendingReqsResponse{Success: false, Error: "method not allowed"})
		return
	}
	auth, err := authenticateRequest(r)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, pendingReqsResponse{Success: false, Error: err.Error()})
		return
	}
	defer auth.Cancel()

	dbrep, err := dbClient.GetFriendRequestsByUser(auth.Context, &pb.GetFriendRequestsByUserReq{
		UserId: auth.UserID,
		Status: 0,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, pendingReqsResponse{Success: false, Error: err.Error()})
		return
	}

	reqs := make([]pendingReqInfo, 0, len(dbrep.Requests))
	for _, pr := range dbrep.Requests {
		reqs = append(reqs, pendingReqInfo{
			RequestID:    pr.Id,
			FromUserID:   pr.FromUserId,
			FromUsername: pr.FromUsername,
			Status:       pr.Status,
			CreatedAt:    pr.CreatedAt,
		})
	}

	writeJSON(w, http.StatusOK, pendingReqsResponse{Success: true, Requests: reqs})
}
