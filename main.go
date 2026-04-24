package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/md5"
	crand "crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// ═══════════════════════════════════════════════════════════════════
// CONFIGURATION & PERSISTENCE
// ═══════════════════════════════════════════════════════════════════

type Config struct {
	ServerName     string
	Motd           string
	Port           string
	WorldPasswords map[string]string
	OpPassword     string
	BannedBlocks   []byte
	Public         bool
	MaxPlayers     int
	VerifyNames    bool
	TerrainURL     string // URL sent to clients for texture pack
	HTTPPort       string // port for built-in HTTP server (serves terrain.png)
	Software       string
}

var (
	serverConfig Config
	serverSalt   string // random salt for heartbeat / name verification
)

func hashPassword(password string) string {
	hash := sha256.Sum256([]byte(password))
	return hex.EncodeToString(hash[:])
}

func generateSalt() string {
	b := make([]byte, 16)
	crand.Read(b)
	return hex.EncodeToString(b)[:16]
}

// verifyPlayerName checks md5(salt + username) == verifyKey.
// This is the ClassiCube name verification scheme.
func verifyPlayerName(username, verifyKey string) bool {
	expected := md5.Sum([]byte(serverSalt + username))
	return hex.EncodeToString(expected[:]) == verifyKey
}

func saveConfig() {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("ServerName=%s\n", serverConfig.ServerName))
	buf.WriteString(fmt.Sprintf("Motd=%s\n", serverConfig.Motd))
	buf.WriteString(fmt.Sprintf("Port=%s\n", strings.TrimPrefix(serverConfig.Port, ":")))
	buf.WriteString(fmt.Sprintf("Public=%v\n", serverConfig.Public))
	buf.WriteString(fmt.Sprintf("MaxPlayers=%d\n", serverConfig.MaxPlayers))
	buf.WriteString(fmt.Sprintf("VerifyNames=%v\n", serverConfig.VerifyNames))
	buf.WriteString(fmt.Sprintf("Software=%s\n", serverConfig.Software))

	if serverConfig.TerrainURL != "" {
		buf.WriteString(fmt.Sprintf("TerrainURL=%s\n", serverConfig.TerrainURL))
	}
	if serverConfig.HTTPPort != "" {
		buf.WriteString(fmt.Sprintf("HTTPPort=%s\n", serverConfig.HTTPPort))
	}

	if serverConfig.OpPassword != "" {
		buf.WriteString(fmt.Sprintf("OpPassword=%s\n", serverConfig.OpPassword))
	}

	if len(serverConfig.BannedBlocks) > 0 {
		var strBlocks []string
		for _, b := range serverConfig.BannedBlocks {
			strBlocks = append(strBlocks, strconv.Itoa(int(b)))
		}
		buf.WriteString(fmt.Sprintf("BannedBlocks=%s\n", strings.Join(strBlocks, ",")))
	}

	for world, passHash := range serverConfig.WorldPasswords {
		buf.WriteString(fmt.Sprintf("pass_%s=%s\n", world, passHash))
	}

	os.WriteFile("server.properties", buf.Bytes(), 0644)
}

func savePortals(portals []Portal, path string) {
	data, _ := json.MarshalIndent(portals, "", "  ")
	os.WriteFile(path, data, 0644)
}

func loadPortals(path string) []Portal {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var portals []Portal
	json.Unmarshal(data, &portals)
	return portals
}

func loadConfig() {
	serverConfig = Config{
		ServerName:     "Go Classic Server",
		Motd:           "Welcome to the server!",
		Port:           ":25565",
		WorldPasswords: make(map[string]string),
		Public:         false,
		MaxPlayers:     128,
		VerifyNames:    true,
		Software:       "GoClassic 1.0",
	}

	f, err := os.Open("server.properties")
	if err != nil {
		defaultWorldHash := hashPassword("letmein")
		defaultOpHash := hashPassword("admin")

		defaultProps := fmt.Sprintf("ServerName=Go Classic Server\nMotd=Welcome to the server!\nPort=25565\nPublic=false\nMaxPlayers=128\nVerifyNames=true\nSoftware=GoClassic 1.0\n# TerrainURL=http://your-ip:8080/terrain.zip\n# HTTPPort=8080\npass_world2=%s\nOpPassword=%s\nBannedBlocks=8,9,10,11\n", defaultWorldHash, defaultOpHash)
		os.WriteFile("server.properties", []byte(defaultProps), 0644)
		serverConfig.WorldPasswords["world2"] = defaultWorldHash
		serverConfig.OpPassword = defaultOpHash
		serverConfig.BannedBlocks = []byte{8, 9, 10, 11}
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || !strings.Contains(line, "=") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		key, val := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])

		switch key {
		case "ServerName":
			serverConfig.ServerName = val
		case "Motd":
			serverConfig.Motd = val
		case "Port":
			serverConfig.Port = ":" + strings.TrimPrefix(val, ":")
		case "OpPassword":
			serverConfig.OpPassword = val
		case "Public":
			serverConfig.Public = strings.EqualFold(val, "true")
		case "MaxPlayers":
			if n, err := strconv.Atoi(val); err == nil && n > 0 {
				serverConfig.MaxPlayers = n
			}
		case "VerifyNames":
			serverConfig.VerifyNames = strings.EqualFold(val, "true")
		case "TerrainURL":
			serverConfig.TerrainURL = val
		case "HTTPPort":
			serverConfig.HTTPPort = strings.TrimPrefix(val, ":")
		case "Software":
			serverConfig.Software = val
		case "BannedBlocks":
			for _, bStr := range strings.Split(val, ",") {
				if b, err := strconv.Atoi(strings.TrimSpace(bStr)); err == nil {
					serverConfig.BannedBlocks = append(serverConfig.BannedBlocks, byte(b))
				}
			}
		default:
			if strings.HasPrefix(key, "pass_") {
				worldName := strings.TrimPrefix(key, "pass_")
				serverConfig.WorldPasswords[worldName] = val
			}
		}
	}
}

// ═══════════════════════════════════════════════════════════════════
// CLASSICUBE HEARTBEAT & SERVER LIST
// ═══════════════════════════════════════════════════════════════════

func startHeartbeat(server *Server) {
	if !serverConfig.Public {
		log.Println("[Heartbeat] Public=false — not sending heartbeats.")
		return
	}

	log.Printf("[Heartbeat] Salt=%s — starting heartbeat to classicube.net", serverSalt)

	go func() {
		ticker := time.NewTicker(45 * time.Second)
		defer ticker.Stop()

		// Send immediately on start, then every 45s
		sendHeartbeat(server)
		for range ticker.C {
			sendHeartbeat(server)
		}
	}()
}

func sendHeartbeat(server *Server) {
	server.mu.RLock()
	playerCount := len(server.players)
	server.mu.RUnlock()

	port := strings.TrimPrefix(serverConfig.Port, ":")

	params := url.Values{}
	params.Set("name", serverConfig.ServerName)
	params.Set("port", port)
	params.Set("users", strconv.Itoa(playerCount))
	params.Set("max", strconv.Itoa(serverConfig.MaxPlayers))
	params.Set("public", "True")
	params.Set("software", serverConfig.Software)
	params.Set("salt", serverSalt)
	params.Set("web", "True")

	// ClassiCube heartbeat accepts GET with query params
	heartbeatURL := "https://www.classicube.net/server/heartbeat/?" + params.Encode()

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(heartbeatURL)
	if err != nil {
		log.Printf("[Heartbeat] Failed: %v", err)
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	result := strings.TrimSpace(string(body))

	if resp.StatusCode == 200 && strings.HasPrefix(result, "http") {
		log.Printf("[Heartbeat] Listed — %d player(s) — %s", playerCount, result)
	} else if resp.StatusCode == 200 {
		// 200 but no URL means an error message from the server
		log.Printf("[Heartbeat] Response: %s", result)
	} else {
		log.Printf("[Heartbeat] HTTP %d: %s", resp.StatusCode, result)
	}
}

// ═══════════════════════════════════════════════════════════════════
// BUILT-IN HTTP SERVER (serves terrain.png as .zip for CPE EnvMapAspect)
// ═══════════════════════════════════════════════════════════════════

var terrainZipCache []byte // cached zip containing terrain.png

func buildTerrainZip() ([]byte, error) {
	pngData, err := os.ReadFile("terrain.png")
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)

	fw, err := zw.Create("terrain.png")
	if err != nil {
		return nil, err
	}
	fw.Write(pngData)
	zw.Close()

	return buf.Bytes(), nil
}

func startHTTPServer() {
	if serverConfig.HTTPPort == "" {
		return
	}

	// Look for terrain.png in the current dir or a textures/ subdir
	terrainPath := "terrain.png"
	if _, err := os.Stat(terrainPath); os.IsNotExist(err) {
		terrainPath = filepath.Join("textures", "terrain.png")
		if _, err := os.Stat(terrainPath); os.IsNotExist(err) {
			log.Printf("[HTTP] terrain.png not found — HTTP server not started.")
			log.Printf("[HTTP] Place terrain.png in the server directory to enable.")
			return
		}
	}

	// Pre-build the zip
	zipData, err := buildTerrainZip()
	if err != nil {
		log.Printf("[HTTP] Failed to build terrain.zip: %v", err)
		return
	}
	terrainZipCache = zipData
	log.Printf("[HTTP] Built terrain.zip (%d bytes) from %s", len(zipData), terrainPath)

	mux := http.NewServeMux()

	// Serve the raw PNG
	mux.HandleFunc("/terrain.png", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		http.ServeFile(w, r, terrainPath)
	})

	// Serve the zipped texture pack (ClassiCube EnvMapAspect expects a .zip)
	mux.HandleFunc("/terrain.zip", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/zip")
		w.Header().Set("Content-Length", strconv.Itoa(len(terrainZipCache)))
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Write(terrainZipCache)
	})

	// Reload endpoint so ops can update the texture without restarting
	mux.HandleFunc("/terrain/reload", func(w http.ResponseWriter, r *http.Request) {
		newZip, err := buildTerrainZip()
		if err != nil {
			http.Error(w, "reload failed: "+err.Error(), 500)
			return
		}
		terrainZipCache = newZip
		log.Printf("[HTTP] terrain.zip reloaded (%d bytes)", len(newZip))
		w.Write([]byte("OK"))
	})

	addr := ":" + serverConfig.HTTPPort
	log.Printf("[HTTP] Serving terrain on %s (/terrain.png, /terrain.zip)", addr)

	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("[HTTP] Server failed: %v", err)
		}
	}()
}

// ═══════════════════════════════════════════════════════════════════
// CPE EnvMapAspect — send texture URL to client
// ═══════════════════════════════════════════════════════════════════

// sendSetTexturePack sends a CPE SetMapEnvUrl (0x28) packet from
// the EnvMapAspect extension — this sets the texture pack URL.
func sendSetTexturePack(conn net.Conn, textureURL string) {
	pkt := make([]byte, 65)
	pkt[0] = 0x28
	copy(pkt[1:65], padString(textureURL))
	conn.Write(pkt)
}

// ═══════════════════════════════════════════════════════════════════
// CUSTOM BLOCK DEFINITIONS  (CPE BlockDefinitions)
// ═══════════════════════════════════════════════════════════════════

// CustomBlock holds every property the CPE DefineBlock packet carries.
// Persisted to customblocks.json and sent to BlockDefinitions-capable clients.
type CustomBlock struct {
	ID            byte   `json:"id"`
	Name          string `json:"name"`
	Solidity      byte   `json:"solidity"`       // 0=walk-through  1=swim-through  2=solid
	Speed         byte   `json:"speed"`          // 1..8  (4 = normal 1.0x)
	TopTex        byte   `json:"top_tex"`        // texture-atlas index
	SideTex       byte   `json:"side_tex"`       // texture-atlas index (all 4 sides)
	BottomTex     byte   `json:"bottom_tex"`     // texture-atlas index
	TransmitLight byte   `json:"transmit_light"` // 0 or 1
	WalkSound     byte   `json:"walk_sound"`     // 0=none 1=wood 2=gravel 3=grass 4=stone 5=metal 6=glass 7=wool 8=sand 9=snow
	FullBright    byte   `json:"full_bright"`    // 0 or 1
	Shape         byte   `json:"shape"`          // 0=sprite  1-16=height in sixteenths
	BlockDraw     byte   `json:"block_draw"`     // 0=opaque 1=transparent(same) 2=transparent(diff) 3=translucent 4=gas
	FogDensity    byte   `json:"fog_density"`
	FogR          byte   `json:"fog_r"`
	FogG          byte   `json:"fog_g"`
	FogB          byte   `json:"fog_b"`
	Fallback      byte   `json:"fallback"` // vanilla block ID for non-CPE clients (0-49)
}

var (
	customBlocks   = make(map[byte]CustomBlock) // key = block ID
	customBlocksMu sync.RWMutex
)

func loadCustomBlocks() {
	data, err := os.ReadFile("customblocks.json")
	if err != nil {
		return
	}

	// Strip UTF-8 BOM if present (common when editing with Notepad on Windows)
	if len(data) >= 3 && data[0] == 0xEF && data[1] == 0xBB && data[2] == 0xBF {
		data = data[3:]
	}

	var list []CustomBlock
	if err := json.Unmarshal(data, &list); err != nil {
		log.Printf("[Blocks] ERROR parsing customblocks.json: %v", err)
		log.Printf("[Blocks] No custom blocks loaded — fix the JSON and restart.")
		return
	}

	customBlocksMu.Lock()
	for _, cb := range list {
		if cb.ID < 50 {
			log.Printf("[Blocks] WARNING: skipping block %d (%q) — ID must be 50-255", cb.ID, cb.Name)
			continue
		}
		cb = clampBlockProperties(cb)
		customBlocks[cb.ID] = cb
		log.Printf("[Blocks] Loaded block %d: %q (tex %d/%d/%d, fb %d)",
			cb.ID, cb.Name, cb.TopTex, cb.SideTex, cb.BottomTex, cb.Fallback)
	}
	customBlocksMu.Unlock()
}

// clampBlockProperties forces every field into its valid range so a
// hand-edited JSON file can never produce a broken DefineBlock packet.
func clampBlockProperties(cb CustomBlock) CustomBlock {
	if cb.Solidity > 2 {
		cb.Solidity = 2
	}
	if cb.Speed < 1 || cb.Speed > 8 {
		cb.Speed = 4
	}
	if cb.TransmitLight > 1 {
		cb.TransmitLight = 0
	}
	if cb.WalkSound > 9 {
		cb.WalkSound = 0
	}
	if cb.FullBright > 1 {
		cb.FullBright = 0
	}
	if cb.Shape > 16 {
		cb.Shape = 16
	}
	if cb.BlockDraw > 4 {
		cb.BlockDraw = 0
	}
	if cb.Fallback > 49 {
		cb.Fallback = 1
	}
	if len(cb.Name) > 64 {
		cb.Name = cb.Name[:64]
	}
	return cb
}

// ── MCGalaxy global.json import ──────────────────────────────────

// mcgBlockDef mirrors the JSON layout MCGalaxy uses in global.json.
type mcgBlockDef struct {
	BlockID     int    `json:"BlockID"`
	Name        string `json:"Name"`
	Speed       int    `json:"Speed"`
	CollideType int    `json:"CollideType"`
	TopTex      int    `json:"TopTex"`
	BottomTex   int    `json:"BottomTex"`
	LeftTex     int    `json:"LeftTex"`
	RightTex    int    `json:"RightTex"`
	FrontTex    int    `json:"FrontTex"`
	BackTex     int    `json:"BackTex"`
	BlocksLight bool   `json:"BlocksLight"`
	WalkSound   int    `json:"WalkSound"`
	FullBright  bool   `json:"FullBright"`
	Shape       int    `json:"Shape"`
	BlockDraw   int    `json:"BlockDraw"`
	FallBack    int    `json:"FallBack"`
	FogDensity  int    `json:"FogDensity"`
	FogR        int    `json:"FogR"`
	FogG        int    `json:"FogG"`
	FogB        int    `json:"FogB"`
	MinX        int    `json:"MinX"`
	MinY        int    `json:"MinY"`
	MinZ        int    `json:"MinZ"`
	MaxX        int    `json:"MaxX"`
	MaxY        int    `json:"MaxY"`
	MaxZ        int    `json:"MaxZ"`
}

// mcgToCustomBlock converts an MCGalaxy block definition into our format.
func mcgToCustomBlock(m mcgBlockDef) CustomBlock {
	// MCGalaxy stores per-face side textures; DefineBlock (0x23) has one
	// SideTex field, so pick LeftTex (they're usually all the same).
	sideTex := byte(m.LeftTex)

	// MCGalaxy: BlocksLight=true means the block BLOCKS light → TransmitLight=0
	transmit := byte(0)
	if !m.BlocksLight {
		transmit = 1
	}

	bright := byte(0)
	if m.FullBright {
		bright = 1
	}

	// MCGalaxy Speed is the raw CPE byte (1-8).
	// A value of 0 in their JSON means "default" → treat as 4 (1.0×).
	speed := byte(m.Speed)
	if speed == 0 {
		speed = 4
	}

	// Ensure fallback is vanilla-safe
	fallback := byte(m.FallBack)
	if fallback > 49 {
		fallback = 1
	}

	return clampBlockProperties(CustomBlock{
		ID:            byte(m.BlockID),
		Name:          m.Name,
		Solidity:      byte(m.CollideType),
		Speed:         speed,
		TopTex:        byte(m.TopTex),
		SideTex:       sideTex,
		BottomTex:     byte(m.BottomTex),
		TransmitLight: transmit,
		WalkSound:     byte(m.WalkSound),
		FullBright:    bright,
		Shape:         byte(m.Shape),
		BlockDraw:     byte(m.BlockDraw),
		FogDensity:    byte(m.FogDensity),
		FogR:          byte(m.FogR),
		FogG:          byte(m.FogG),
		FogB:          byte(m.FogB),
		Fallback:      fallback,
	})
}

// importMCGalaxyBlocks reads an MCGalaxy-format JSON file and merges
// the blocks into the custom blocks map. Returns counts of imported
// and skipped blocks plus any error.
func importMCGalaxyBlocks(path string) (imported, skipped int, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, err
	}

	// Strip UTF-8 BOM
	if len(data) >= 3 && data[0] == 0xEF && data[1] == 0xBB && data[2] == 0xBF {
		data = data[3:]
	}

	var defs []mcgBlockDef
	if err := json.Unmarshal(data, &defs); err != nil {
		return 0, 0, fmt.Errorf("JSON parse error: %w", err)
	}

	customBlocksMu.Lock()
	defer customBlocksMu.Unlock()

	for _, m := range defs {
		if m.BlockID < 50 || m.BlockID > 255 {
			log.Printf("[Import] Skipping block %d (%q) — ID out of range 50-255", m.BlockID, m.Name)
			skipped++
			continue
		}
		cb := mcgToCustomBlock(m)
		customBlocks[cb.ID] = cb
		imported++
		log.Printf("[Import] Imported block %d: %q (tex %d/%d/%d, fb %d)",
			cb.ID, cb.Name, cb.TopTex, cb.SideTex, cb.BottomTex, cb.Fallback)
	}

	return imported, skipped, nil
}

func saveCustomBlocks() {
	customBlocksMu.RLock()
	list := make([]CustomBlock, 0, len(customBlocks))
	for _, cb := range customBlocks {
		list = append(list, cb)
	}
	customBlocksMu.RUnlock()
	data, _ := json.MarshalIndent(list, "", "  ")
	os.WriteFile("customblocks.json", data, 0644)
}

// vanillaBlockNames maps standard Classic block IDs to human-readable names.
var vanillaBlockNames = map[byte]string{
	0: "Air", 1: "Stone", 2: "Grass", 3: "Dirt", 4: "Cobblestone",
	5: "Planks", 6: "Sapling", 7: "Bedrock", 8: "Water", 9: "Still Water",
	10: "Lava", 11: "Still Lava", 12: "Sand", 13: "Gravel", 14: "Gold Ore",
	15: "Iron Ore", 16: "Coal Ore", 17: "Log", 18: "Leaves", 19: "Sponge",
	20: "Glass", 21: "Red Wool", 22: "Orange Wool", 23: "Yellow Wool",
	24: "Lime Wool", 25: "Green Wool", 26: "Teal Wool", 27: "Aqua Wool",
	28: "Cyan Wool", 29: "Blue Wool", 30: "Indigo Wool", 31: "Violet Wool",
	32: "Magenta Wool", 33: "Pink Wool", 34: "Black Wool", 35: "Gray Wool",
	36: "White Wool", 37: "Dandelion", 38: "Rose", 39: "Brown Mushroom",
	40: "Red Mushroom", 41: "Gold Block", 42: "Iron Block", 43: "Double Slab",
	44: "Slab", 45: "Brick", 46: "TNT", 47: "Bookshelf", 48: "Mossy Cobblestone",
	49: "Obsidian",
	// CPE CustomBlocks (50-65)
	50: "Cobblestone Slab", 51: "Rope", 52: "Sandstone", 53: "Snow",
	54: "Fire", 55: "Light Pink", 56: "Forest Green", 57: "Brown",
	58: "Deep Blue", 59: "Turquoise", 60: "Ice", 61: "Ceramic Tile",
	62: "Magma", 63: "Pillar", 64: "Crate", 65: "Stone Brick",
}

// blockName returns a human-readable name for a block ID.
// Checks custom blocks first, then vanilla names, then falls back to the numeric ID.
func blockName(id byte) string {
	customBlocksMu.RLock()
	if cb, ok := customBlocks[id]; ok {
		customBlocksMu.RUnlock()
		return cb.Name
	}
	customBlocksMu.RUnlock()

	if name, ok := vanillaBlockNames[id]; ok {
		return name
	}
	return fmt.Sprintf("Block %d", id)
}

// sendDefineBlock sends a CPE DefineBlock (0x23) packet to a single connection.
func sendDefineBlock(conn net.Conn, cb CustomBlock) {
	pkt := make([]byte, 80)
	pkt[0] = 0x23
	pkt[1] = cb.ID
	copy(pkt[2:66], padString(cb.Name))
	pkt[66] = cb.Solidity
	pkt[67] = cb.Speed
	pkt[68] = cb.TopTex
	pkt[69] = cb.SideTex
	pkt[70] = cb.BottomTex
	pkt[71] = cb.TransmitLight
	pkt[72] = cb.WalkSound
	pkt[73] = cb.FullBright
	pkt[74] = cb.Shape
	pkt[75] = cb.BlockDraw
	pkt[76] = cb.FogDensity
	pkt[77] = cb.FogR
	pkt[78] = cb.FogG
	pkt[79] = cb.FogB
	conn.Write(pkt)
}

// sendRemoveBlockDef sends a CPE RemoveBlockDefinition (0x24) packet.
func sendRemoveBlockDef(conn net.Conn, id byte) {
	conn.Write([]byte{0x24, id})
}

// sendAllCustomBlocks sends every stored definition to a connection.
func sendAllCustomBlocks(conn net.Conn) {
	customBlocksMu.RLock()
	defer customBlocksMu.RUnlock()
	for _, cb := range customBlocks {
		sendDefineBlock(conn, cb)
	}
}

// broadcastDefineBlock pushes a definition to every connected BlockDefs client.
func (s *Server) broadcastDefineBlock(cb CustomBlock) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, p := range s.players {
		if p.SupportsBlockDefs {
			sendDefineBlock(p.Conn, cb)
		}
	}
}

// broadcastRemoveBlock pushes a remove to every connected BlockDefs client.
func (s *Server) broadcastRemoveBlock(id byte) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, p := range s.players {
		if p.SupportsBlockDefs {
			sendRemoveBlockDef(p.Conn, id)
		}
	}
}

// ═══════════════════════════════════════════════════════════════════
// PLAYER DB PERSISTENCE
// ═══════════════════════════════════════════════════════════════════

// PlayerState is used only for JSON migration.
type PlayerState struct {
	World          string            `json:"world"`
	X              int16             `json:"x"`
	Y              int16             `json:"y"`
	Z              int16             `json:"z"`
	Yaw            byte              `json:"yaw"`
	Pitch          byte              `json:"pitch"`
	SavedPasswords map[string]string `json:"saved_passwords"`
}

// loadPlayerState reads a player's saved state from SQLite.
func loadPlayerState(username string) (world string, x, y, z int16, yaw, pitch, heldBlock byte, passwords map[string]string, ok bool) {
	if serverDB == nil {
		return
	}
	passwords = make(map[string]string)

	err := serverDB.QueryRow(
		"SELECT world, x, y, z, yaw, pitch, held_block FROM players WHERE username=?", username,
	).Scan(&world, &x, &y, &z, &yaw, &pitch, &heldBlock)
	if err != nil {
		return
	}
	ok = true

	rows, err := serverDB.Query("SELECT world_name, password_hash FROM player_passwords WHERE username=?", username)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var wn, ph string
		rows.Scan(&wn, &ph)
		passwords[wn] = ph
	}
	return
}

// updatePlayerState saves the player's current state to SQLite.
func updatePlayerState(p *Player) {
	if p.World == nil || serverDB == nil {
		return
	}

	p.mu.Lock()
	hb := p.HeldBlock
	savedPws := make(map[string]string)
	for k, v := range p.SavedPasswords {
		savedPws[k] = v
	}
	p.mu.Unlock()

	serverDB.Exec(`
		INSERT INTO players(username, world, x, y, z, yaw, pitch, held_block)
		VALUES(?,?,?,?,?,?,?,?)
		ON CONFLICT(username) DO UPDATE SET
			world=excluded.world, x=excluded.x, y=excluded.y, z=excluded.z,
			yaw=excluded.yaw, pitch=excluded.pitch, held_block=excluded.held_block
	`, p.Username, p.World.Name, p.X, p.Y, p.Z, p.Yaw, p.Pitch, hb)

	// Update saved passwords
	for wn, ph := range savedPws {
		serverDB.Exec(`
			INSERT INTO player_passwords(username, world_name, password_hash)
			VALUES(?,?,?)
			ON CONFLICT(username, world_name) DO UPDATE SET password_hash=excluded.password_hash
		`, p.Username, wn, ph)
	}
}

// ═══════════════════════════════════════════════════════════════════
// BAN LIST
// ═══════════════════════════════════════════════════════════════════

type BanEntry struct {
	Username string `json:"username"`
	Reason   string `json:"reason"`
	BannedBy string `json:"banned_by"`
	Time     string `json:"time"`
	IP       string `json:"ip"`
}

var (
	bannedPlayers = make(map[string]BanEntry)
	banMutex      sync.RWMutex
)

func loadBanList() {
	data, err := os.ReadFile("banned.json")
	if err != nil {
		return
	}
	var list []BanEntry
	if json.Unmarshal(data, &list) == nil {
		banMutex.Lock()
		for _, b := range list {
			bannedPlayers[strings.ToLower(b.Username)] = b
		}
		banMutex.Unlock()
	}
}

func saveBanList() {
	banMutex.RLock()
	list := make([]BanEntry, 0, len(bannedPlayers))
	for _, b := range bannedPlayers {
		list = append(list, b)
	}
	banMutex.RUnlock()
	data, _ := json.MarshalIndent(list, "", "  ")
	os.WriteFile("banned.json", data, 0644)
}

func isPlayerBanned(username string) (BanEntry, bool) {
	banMutex.RLock()
	defer banMutex.RUnlock()
	b, ok := bannedPlayers[strings.ToLower(username)]
	return b, ok
}

// ═══════════════════════════════════════════════════════════════════
// BLOCK HISTORY — SQLite-backed per-world change log
// ═══════════════════════════════════════════════════════════════════

type BlockChange struct {
	Time     int64
	Player   string
	World    string
	X        int16
	Y        int16
	Z        int16
	OldBlock byte
	NewBlock byte
}

var serverDB *sql.DB

func initDatabase() {
	var err error
	serverDB, err = sql.Open("sqlite", "server.db?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		log.Fatalf("[DB] Failed to open database: %v", err)
	}

	// WAL mode + pragmas for performance
	serverDB.Exec("PRAGMA journal_mode=WAL")
	serverDB.Exec("PRAGMA synchronous=NORMAL")
	serverDB.Exec("PRAGMA cache_size=-8000") // 8MB cache

	// ── Block history table ──
	_, err = serverDB.Exec(`
		CREATE TABLE IF NOT EXISTS block_changes (
			id        INTEGER PRIMARY KEY AUTOINCREMENT,
			time      INTEGER NOT NULL,
			player    TEXT    NOT NULL,
			world     TEXT    NOT NULL,
			x         INTEGER NOT NULL,
			y         INTEGER NOT NULL,
			z         INTEGER NOT NULL,
			old_block INTEGER NOT NULL,
			new_block INTEGER NOT NULL
		)
	`)
	if err != nil {
		log.Fatalf("[DB] Failed to create block_changes table: %v", err)
	}
	serverDB.Exec("CREATE INDEX IF NOT EXISTS idx_block_pos ON block_changes(world, x, y, z, time DESC)")
	serverDB.Exec("CREATE INDEX IF NOT EXISTS idx_player_time ON block_changes(world, player COLLATE NOCASE, time DESC)")
	serverDB.Exec("CREATE INDEX IF NOT EXISTS idx_time ON block_changes(time)")

	// ── Players table ──
	serverDB.Exec(`
		CREATE TABLE IF NOT EXISTS players (
			username   TEXT PRIMARY KEY,
			world      TEXT NOT NULL DEFAULT 'hub',
			x          INTEGER NOT NULL DEFAULT 0,
			y          INTEGER NOT NULL DEFAULT 0,
			z          INTEGER NOT NULL DEFAULT 0,
			yaw        INTEGER NOT NULL DEFAULT 0,
			pitch      INTEGER NOT NULL DEFAULT 0,
			held_block INTEGER NOT NULL DEFAULT 1,
			last_seen  INTEGER NOT NULL DEFAULT 0
		)
	`)

	// ── Player saved passwords table ──
	serverDB.Exec(`
		CREATE TABLE IF NOT EXISTS player_passwords (
			username    TEXT NOT NULL,
			world_name  TEXT NOT NULL,
			password_hash TEXT NOT NULL,
			PRIMARY KEY (username, world_name)
		)
	`)

	// Auto-migrate old data
	migrateJSONHistory()
	migrateJSONPlayers()

	// Migrate old blockhistory.db if present (from before the rename)
	if _, err := os.Stat("blockhistory.db"); err == nil {
		log.Printf("[DB] NOTE: Old blockhistory.db found. Data has been migrated to server.db.")
		log.Printf("[DB] You can delete blockhistory.db to save space.")
	}

	var histCount int64
	serverDB.QueryRow("SELECT COUNT(*) FROM block_changes").Scan(&histCount)
	var playerCount int64
	serverDB.QueryRow("SELECT COUNT(*) FROM players").Scan(&playerCount)
	log.Printf("[DB] Ready — %d block changes, %d players", histCount, playerCount)
}

func migrateJSONPlayers() {
	data, err := os.ReadFile("players.json")
	if err != nil {
		return
	}

	var oldDB map[string]PlayerState
	if json.Unmarshal(data, &oldDB) != nil || len(oldDB) == 0 {
		return
	}

	log.Printf("[DB] Migrating %d players from players.json to SQLite...", len(oldDB))

	tx, err := serverDB.Begin()
	if err != nil {
		return
	}

	playerStmt, _ := tx.Prepare(`
		INSERT OR IGNORE INTO players(username, world, x, y, z, yaw, pitch, held_block, last_seen)
		VALUES(?,?,?,?,?,?,?,1,?)
	`)
	pwStmt, _ := tx.Prepare(`
		INSERT OR IGNORE INTO player_passwords(username, world_name, password_hash)
		VALUES(?,?,?)
	`)

	now := time.Now().Unix()
	for username, state := range oldDB {
		playerStmt.Exec(username, state.World, state.X, state.Y, state.Z, state.Yaw, state.Pitch, now)
		for wn, ph := range state.SavedPasswords {
			pwStmt.Exec(username, wn, ph)
		}
	}
	playerStmt.Close()
	pwStmt.Close()
	tx.Commit()

	os.Rename("players.json", "players.json.migrated")
	log.Printf("[DB] Migrated %d players — renamed players.json to players.json.migrated", len(oldDB))
}

func migrateJSONHistory() {
	entries, err := os.ReadDir("blockhistory")
	if err != nil {
		return
	}

	type jsonChange struct {
		Time     int64  `json:"t"`
		Player   string `json:"p"`
		X        int16  `json:"x"`
		Y        int16  `json:"y"`
		Z        int16  `json:"z"`
		OldBlock byte   `json:"ob"`
		NewBlock byte   `json:"nb"`
	}

	total := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		worldName := strings.TrimSuffix(e.Name(), ".json")
		data, err := os.ReadFile("blockhistory/" + e.Name())
		if err != nil {
			continue
		}
		var changes []jsonChange
		if json.Unmarshal(data, &changes) != nil || len(changes) == 0 {
			continue
		}

		log.Printf("[History] Migrating %d entries from %s to SQLite...", len(changes), e.Name())

		tx, err := serverDB.Begin()
		if err != nil {
			continue
		}
		stmt, _ := tx.Prepare("INSERT INTO block_changes(time,player,world,x,y,z,old_block,new_block) VALUES(?,?,?,?,?,?,?,?)")
		for _, c := range changes {
			stmt.Exec(c.Time, c.Player, worldName, c.X, c.Y, c.Z, c.OldBlock, c.NewBlock)
		}
		stmt.Close()
		tx.Commit()
		total += len(changes)

		// Rename the old file so it's not migrated again
		os.Rename("blockhistory/"+e.Name(), "blockhistory/"+e.Name()+".migrated")
	}
	if total > 0 {
		log.Printf("[History] Migrated %d total entries from JSON to SQLite", total)
	}
}

func recordBlockChange(worldName, player string, x, y, z int16, oldBlock, newBlock byte) {
	if serverDB == nil {
		return
	}
	serverDB.Exec(
		"INSERT INTO block_changes(time,player,world,x,y,z,old_block,new_block) VALUES(?,?,?,?,?,?,?,?)",
		time.Now().Unix(), player, worldName, x, y, z, oldBlock, newBlock,
	)
}

// getBlockHistoryAt returns the history for a specific coordinate, newest first.
func getBlockHistoryAt(worldName string, x, y, z int16, limit int) []BlockChange {
	if serverDB == nil {
		return nil
	}
	rows, err := serverDB.Query(
		"SELECT time, player, old_block, new_block FROM block_changes WHERE world=? AND x=? AND y=? AND z=? ORDER BY time DESC LIMIT ?",
		worldName, x, y, z, limit,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []BlockChange
	for rows.Next() {
		var c BlockChange
		c.World = worldName
		c.X, c.Y, c.Z = x, y, z
		rows.Scan(&c.Time, &c.Player, &c.OldBlock, &c.NewBlock)
		result = append(result, c)
	}
	return result
}

// getPlayerChanges returns all changes by a player in a world since a given time, newest first.
func getPlayerChanges(worldName, player string, since int64) []BlockChange {
	if serverDB == nil {
		return nil
	}
	rows, err := serverDB.Query(
		"SELECT time, player, x, y, z, old_block, new_block FROM block_changes WHERE world=? AND player=? COLLATE NOCASE AND time>=? ORDER BY time DESC",
		worldName, player, since,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var result []BlockChange
	for rows.Next() {
		var c BlockChange
		c.World = worldName
		rows.Scan(&c.Time, &c.Player, &c.X, &c.Y, &c.Z, &c.OldBlock, &c.NewBlock)
		result = append(result, c)
	}
	return result
}

// pruneBlockHistory removes entries older than maxAge.
func pruneBlockHistory(maxAge time.Duration) {
	if serverDB == nil {
		return
	}
	cutoff := time.Now().Add(-maxAge).Unix()
	result, err := serverDB.Exec("DELETE FROM block_changes WHERE time < ?", cutoff)
	if err != nil {
		return
	}
	if n, _ := result.RowsAffected(); n > 0 {
		log.Printf("[History] Pruned %d entries older than %v", n, maxAge)
		serverDB.Exec("PRAGMA optimize")
	}
}

func formatTimeAgo(unix int64) string {
	d := time.Since(time.Unix(unix, 0))
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}

// ═══════════════════════════════════════════════════════════════════
// PLUGIN INTERFACE
// ═══════════════════════════════════════════════════════════════════

type EventResult int

const (
	EventContinue EventResult = iota
	EventCancel
)

type Plugin interface {
	Name() string
	OnLoad(s *Server)
	OnPlayerJoin(p *Player, w *World) EventResult
	OnPlayerLeave(p *Player, w *World)
	OnChat(p *Player, msg string) EventResult
	OnBlockChange(p *Player, x, y, z int16, blockID byte) EventResult
	OnPlayerMove(p *Player, x, y, z int16, yaw, pitch byte)
}

type BasePlugin struct{}

func (b *BasePlugin) OnLoad(s *Server)                             {}
func (b *BasePlugin) OnPlayerJoin(p *Player, w *World) EventResult { return EventContinue }
func (b *BasePlugin) OnPlayerLeave(p *Player, w *World)            {}
func (b *BasePlugin) OnChat(p *Player, msg string) EventResult     { return EventContinue }
func (b *BasePlugin) OnBlockChange(p *Player, x, y, z int16, id byte) EventResult {
	return EventContinue
}
func (b *BasePlugin) OnPlayerMove(p *Player, x, y, z int16, yaw, pitch byte) {}

// ═══════════════════════════════════════════════════════════════════
// WORLD & LEVEL I/O
// ═══════════════════════════════════════════════════════════════════

type Portal struct {
	MinX, MinY, MinZ int16
	MaxX, MaxY, MaxZ int16
	TargetWorld      string
}

type World struct {
	Name                   string
	Width, Height, Length  int16
	Blocks                 []byte
	SpawnX, SpawnY, SpawnZ int16
	SpawnYaw, SpawnPitch   byte
	Portals                []Portal
	mu                     sync.RWMutex
}

func (w *World) blockIndex(x, y, z int) int {
	return y*int(w.Width)*int(w.Length) + z*int(w.Width) + x
}

func (w *World) GetBlock(x, y, z int) byte {
	w.mu.RLock()
	defer w.mu.RUnlock()
	idx := w.blockIndex(x, y, z)
	if idx < 0 || idx >= len(w.Blocks) {
		return 0
	}
	return w.Blocks[idx]
}

func (w *World) SetBlock(x, y, z int, id byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	idx := w.blockIndex(x, y, z)
	if idx >= 0 && idx < len(w.Blocks) {
		w.Blocks[idx] = id
	}
}

func LoadMCGalaxyLevel(path string) (*World, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("gzip open: %w", err)
	}
	defer gz.Close()

	data, err := io.ReadAll(gz)
	if err != nil {
		return nil, fmt.Errorf("gzip read: %w", err)
	}
	if len(data) < 8 {
		return nil, fmt.Errorf("level file too short")
	}

	magic := binary.LittleEndian.Uint16(data[0:2])
	if magic != 1873 && magic != 1874 {
		return nil, fmt.Errorf("bad MCGalaxy magic %d (expected 1873 or 1874)", magic)
	}

	width := int16(binary.LittleEndian.Uint16(data[2:4]))
	depth := int16(binary.LittleEndian.Uint16(data[4:6]))
	height := int16(binary.LittleEndian.Uint16(data[6:8]))

	var spawnX, spawnY, spawnZ int16
	var spawnYaw, spawnPitch byte
	var blockOffset int

	if magic == 1873 {
		blockOffset = 8
		spawnX = width / 2 * 32
		spawnY = (height/2 + 2) * 32
		spawnZ = depth / 2 * 32
	} else {
		blockOffset = 16
		if len(data) < 16 {
			return nil, fmt.Errorf("v2 level file missing spawn header")
		}
		spawnX = int16(binary.LittleEndian.Uint16(data[8:10])) * 32
		spawnZ = int16(binary.LittleEndian.Uint16(data[10:12])) * 32
		spawnY = int16(binary.LittleEndian.Uint16(data[12:14])) * 32
		spawnYaw = data[14]
		spawnPitch = data[15]
	}

	expected := int(width) * int(height) * int(depth)
	blocks := make([]byte, expected)
	if len(data)-blockOffset >= expected {
		copy(blocks, data[blockOffset:blockOffset+expected])
	}

	return &World{
		Width:      width,
		Height:     height,
		Length:     depth,
		Blocks:     blocks,
		SpawnX:     spawnX,
		SpawnY:     spawnY,
		SpawnZ:     spawnZ,
		SpawnYaw:   spawnYaw,
		SpawnPitch: spawnPitch,
	}, nil
}

func SaveMCGalaxyLevel(w *World, path string) error {
	os.MkdirAll("levels", 0755)
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	gz := gzip.NewWriter(f)
	defer gz.Close()

	header := make([]byte, 16)
	binary.LittleEndian.PutUint16(header[0:2], 1874)
	binary.LittleEndian.PutUint16(header[2:4], uint16(w.Width))
	binary.LittleEndian.PutUint16(header[4:6], uint16(w.Length))
	binary.LittleEndian.PutUint16(header[6:8], uint16(w.Height))

	binary.LittleEndian.PutUint16(header[8:10], uint16(w.SpawnX/32))
	binary.LittleEndian.PutUint16(header[10:12], uint16(w.SpawnZ/32))
	binary.LittleEndian.PutUint16(header[12:14], uint16(w.SpawnY/32))
	header[14] = w.SpawnYaw
	header[15] = w.SpawnPitch

	gz.Write(header)
	w.mu.RLock()
	gz.Write(w.Blocks)
	w.mu.RUnlock()
	return nil
}

// ═══════════════════════════════════════════════════════════════════
// SERVER
// ═══════════════════════════════════════════════════════════════════

type Server struct {
	worlds  map[string]*World
	players map[string]*Player
	plugins []Plugin
	usedIDs [128]bool // tracks which player IDs (0-127) are in use
	mu      sync.RWMutex
}

func NewServer() *Server {
	return &Server{
		worlds:  make(map[string]*World),
		players: make(map[string]*Player),
	}
}

// allocateID finds the lowest unused player ID (0-127).
// Must be called with s.mu held for writing.
func (s *Server) allocateID() byte {
	for i := 0; i < 128; i++ {
		if !s.usedIDs[i] {
			s.usedIDs[i] = true
			return byte(i)
		}
	}
	return 0 // fallback, shouldn't happen with MaxPlayers <= 128
}

// freeID releases a player ID back to the pool.
// Must be called with s.mu held for writing.
func (s *Server) freeID(id byte) {
	if id < 128 {
		s.usedIDs[id] = false
	}
}

func (s *Server) RegisterPlugin(p Plugin) {
	s.plugins = append(s.plugins, p)
	p.OnLoad(s)
}

func (s *Server) AddWorld(name string, w *World) {
	w.Name = name
	s.mu.Lock()
	s.worlds[name] = w
	s.mu.Unlock()
	log.Printf("[World]  %q registered (%dx%dx%d)", name, w.Width, w.Height, w.Length)
}

func (s *Server) GetWorld(name string) (*World, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	w, ok := s.worlds[name]
	return w, ok
}

// ═══════════════════════════════════════════════════════════════════
// PLAYER
// ═══════════════════════════════════════════════════════════════════

type Player struct {
	Conn                net.Conn
	ID                  byte // unique player ID (0-127), 255 = self
	Username            string
	World               *World
	Server              *Server
	X, Y, Z             int16
	Yaw, Pitch          byte
	Authenticated       map[string]bool
	SavedPasswords      map[string]string
	PendingWorld        *World
	SupportsCPE         bool
	SupportsBlockDefs   bool
	SupportsHeldBlock   bool
	SupportsTexturePack bool
	IsOp                bool
	InspectMode         bool
	HeldBlock           byte // last block placed / set via /hand
	mu                  sync.Mutex
}

func (p *Player) SendMessage(msg string) {
	pkt := make([]byte, 66)
	pkt[0] = 0x0D
	pkt[1] = 0xFF
	copy(pkt[2:], padString(msg))
	p.Conn.Write(pkt)
}

// ── Player visibility protocol ───────────────────────────────────

// sendSpawnPlayer sends a Spawn Player (0x07) packet to conn.
// playerID 255 means "this is you" (the receiving player's own entity).
func sendSpawnPlayer(conn net.Conn, playerID byte, name string, x, y, z int16, yaw, pitch byte) {
	pkt := make([]byte, 74)
	pkt[0] = 0x07
	pkt[1] = playerID
	copy(pkt[2:66], padString(name))
	binary.BigEndian.PutUint16(pkt[66:68], uint16(x))
	binary.BigEndian.PutUint16(pkt[68:70], uint16(y))
	binary.BigEndian.PutUint16(pkt[70:72], uint16(z))
	pkt[72] = yaw
	pkt[73] = pitch
	conn.Write(pkt)
}

// sendDespawnPlayer sends a Despawn Player (0x0C) packet.
func sendDespawnPlayer(conn net.Conn, playerID byte) {
	conn.Write([]byte{0x0C, playerID})
}

// sendPositionUpdate sends a Position/Orientation (0x08) packet for another player.
func sendPositionUpdate(conn net.Conn, playerID byte, x, y, z int16, yaw, pitch byte) {
	pkt := make([]byte, 10)
	pkt[0] = 0x08
	pkt[1] = playerID
	binary.BigEndian.PutUint16(pkt[2:4], uint16(x))
	binary.BigEndian.PutUint16(pkt[4:6], uint16(y))
	binary.BigEndian.PutUint16(pkt[6:8], uint16(z))
	pkt[8] = yaw
	pkt[9] = pitch
	conn.Write(pkt)
}

// spawnPlayerInWorld sends Spawn packets: existing players see the new player,
// and the new player sees all existing players already in the world.
func (s *Server) spawnPlayerInWorld(p *Player) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, other := range s.players {
		if other == p || other.World != p.World {
			continue
		}
		// Tell existing player about the new player
		sendSpawnPlayer(other.Conn, p.ID, p.Username, p.X, p.Y, p.Z, p.Yaw, p.Pitch)
		// Tell the new player about the existing player
		sendSpawnPlayer(p.Conn, other.ID, other.Username, other.X, other.Y, other.Z, other.Yaw, other.Pitch)
	}
}

// despawnPlayerFromWorld sends Despawn packets to all players in p's current world.
func (s *Server) despawnPlayerFromWorld(p *Player) {
	if p.World == nil {
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, other := range s.players {
		if other == p || other.World != p.World {
			continue
		}
		// Tell other players that this player is gone
		sendDespawnPlayer(other.Conn, p.ID)
		// Tell the leaving player to remove the other player's model
		sendDespawnPlayer(p.Conn, other.ID)
	}
}

// broadcastPosition sends a player's position/orientation to all other
// players in the same world.
func (s *Server) broadcastPosition(p *Player) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, other := range s.players {
		if other == p || other.World != p.World {
			continue
		}
		sendPositionUpdate(other.Conn, p.ID, p.X, p.Y, p.Z, p.Yaw, p.Pitch)
	}
}

// getFallbackBlock returns a vanilla-safe block for non-CPE clients.
// User-defined custom blocks are checked first for their configured fallback,
// then the built-in CPE 50-65 table is used.
func getFallbackBlock(b byte) byte {
	customBlocksMu.RLock()
	if cb, ok := customBlocks[b]; ok {
		customBlocksMu.RUnlock()
		return cb.Fallback
	}
	customBlocksMu.RUnlock()

	switch b {
	case 50:
		return 44 // Cobblestone Slab -> Slab
	case 51:
		return 5 // Rope -> Planks
	case 52:
		return 12 // Sandstone -> Sand
	case 53:
		return 36 // Snow -> White Wool
	case 54:
		return 10 // Fire -> Lava
	case 55:
		return 33 // Light Pink -> Pink Wool
	case 56:
		return 25 // Forest Green -> Green Wool
	case 57:
		return 3 // Brown -> Dirt
	case 58:
		return 28 // Deep Blue -> Blue Wool
	case 59:
		return 27 // Turquoise -> Cyan Wool
	case 60:
		return 20 // Ice -> Glass
	case 61:
		return 42 // Ceramic Tile -> Iron
	case 62:
		return 10 // Magma -> Lava
	case 63:
		return 1 // Pillar -> Stone
	case 64:
		return 5 // Crate -> Planks
	case 65:
		return 1 // Stone Brick -> Stone
	default:
		if b > 49 {
			return 1
		}
		return b
	}
}

func sendBlockUpdate(conn net.Conn, x, y, z int16, block byte, supportsCPE bool) {
	if !supportsCPE && block > 49 {
		block = getFallbackBlock(block)
	}
	pkt := make([]byte, 8)
	pkt[0] = 0x06 // Set Block
	binary.BigEndian.PutUint16(pkt[1:3], uint16(x))
	binary.BigEndian.PutUint16(pkt[3:5], uint16(y))
	binary.BigEndian.PutUint16(pkt[5:7], uint16(z))
	pkt[7] = block
	conn.Write(pkt)
}

// sendSetHeldBlock sends a CPE HeldBlock (0x14) packet to set the
// block the player is holding in their hand.
func sendSetHeldBlock(conn net.Conn, blockID byte, preventChange byte) {
	conn.Write([]byte{0x14, blockID, preventChange})
}

func (p *Player) ChangeWorld(target *World, useSavedPos bool) {
	pw, hasPw := serverConfig.WorldPasswords[target.Name]
	if hasPw && pw != "" {
		p.mu.Lock()
		authed := p.Authenticated[target.Name]
		if !authed {
			if savedPw, ok := p.SavedPasswords[target.Name]; ok && savedPw == pw {
				p.Authenticated[target.Name] = true
				authed = true
			} else {
				p.PendingWorld = target
				p.mu.Unlock()
				p.SendMessage("&eThis world requires a password.")
				p.SendMessage("&eType /pass <password> to enter.")
				return
			}
		}
		p.mu.Unlock()
	}

	for _, pl := range p.Server.plugins {
		if pl.OnPlayerJoin(p, target) == EventCancel {
			p.SendMessage("&cAccess denied.")
			return
		}
	}

	if p.World != nil {
		// Despawn this player from everyone in the old world
		p.Server.despawnPlayerFromWorld(p)
		for _, pl := range p.Server.plugins {
			pl.OnPlayerLeave(p, p.World)
		}
	}

	p.World = target
	sendMapToPlayer(p.Conn, target, p.SupportsCPE)

	sx, sy, sz := target.SpawnX, target.SpawnY, target.SpawnZ
	syaw, spitch := target.SpawnYaw, target.SpawnPitch

	if useSavedPos {
		sx, sy, sz = p.X, p.Y, p.Z
		syaw, spitch = p.Yaw, p.Pitch
	} else {
		p.X, p.Y, p.Z = sx, sy, sz
		p.Yaw, p.Pitch = syaw, spitch
	}

	// Spawn self (ID 255 = "this is you")
	sendSpawnPlayer(p.Conn, 255, p.Username, sx, sy, sz, syaw, spitch)

	// Send texture pack URL after the map is loaded (ClassiCube ignores it before)
	if p.SupportsTexturePack && serverConfig.TerrainURL != "" {
		sendSetTexturePack(p.Conn, serverConfig.TerrainURL)
	}

	// Spawn other players in this world for us, and us for them
	p.Server.spawnPlayerInWorld(p)

	updatePlayerState(p)

	welcomePath := "levels/" + target.Name + ".welcome.txt"
	if data, err := os.ReadFile(welcomePath); err == nil {
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line != "" {
				p.SendMessage(line)
			}
		}
	}
}

// ═══════════════════════════════════════════════════════════════════
// CONNECTION HANDLER & COMMANDS
// ═══════════════════════════════════════════════════════════════════

var clientPacketSizes = map[byte]int{
	0x05: 8,
	0x08: 9,
	0x0D: 65,
	0x10: 66, // ExtInfo
	0x11: 68, // ExtEntry
	0x13: 1,  // CustomBlockSupportLevel
}

func handleConnection(conn net.Conn, server *Server) {
	defer conn.Close()

	buf := make([]byte, 131)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return
	}

	username := strings.TrimRight(string(buf[2:66]), " ")
	if username == "" {
		username = "Player"
	}
	verifyKey := strings.TrimRight(string(buf[66:130]), " ")

	cpe := buf[130] == 0x42
	magic := byte(0x00)
	if cpe {
		magic = 0x42
	}

	// Name verification (when Public + VerifyNames are enabled)
	if serverConfig.Public && serverConfig.VerifyNames && serverSalt != "" {
		if !verifyPlayerName(username, verifyKey) {
			writePacket00(conn, 7, serverConfig.ServerName, "Name verification failed!", 0x00)
			disconnectPlayer(conn, "&cName verification failed. Join via classicube.net.")
			return
		}
	}

	// Ban check
	if ban, banned := isPlayerBanned(username); banned {
		writePacket00(conn, 7, serverConfig.ServerName, "You are banned!", 0x00)
		reason := "Banned"
		if ban.Reason != "" {
			reason = "Banned: " + ban.Reason
		}
		disconnectPlayer(conn, "&c"+reason)
		log.Printf("[Ban] Rejected banned player %s (%s)", username, ban.Reason)
		return
	}

	// Server identification MUST be sent before the CPE handshake
	writePacket00(conn, 7, serverConfig.ServerName, serverConfig.Motd, magic)

	// ── CPE Handshake ──────────────────────────────────────────
	clientSupportsCustomBlocks := false
	clientSupportsBlockDefs := false
	clientSupportsHeldBlock := false
	clientSupportsTexturePack := false

	if cpe {
		// Advertise four extensions
		writeUint8(conn, 0x10) // ExtInfo
		writeString(conn, serverConfig.ServerName)
		writeInt16(conn, 4) // 4 extensions

		writeUint8(conn, 0x11) // ExtEntry 1
		writeString(conn, "CustomBlocks")
		writeInt32(conn, 1)

		writeUint8(conn, 0x11) // ExtEntry 2
		writeString(conn, "BlockDefinitions")
		writeInt32(conn, 1)

		writeUint8(conn, 0x11) // ExtEntry 3
		writeString(conn, "HeldBlock")
		writeInt32(conn, 1)

		writeUint8(conn, 0x11) // ExtEntry 4
		writeString(conn, "EnvMapAspect")
		writeInt32(conn, 1)

		// Read the client's extension list
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		expectedEntries := -1
		entriesRead := 0

		for expectedEntries == -1 || entriesRead < expectedEntries {
			pidBuf := make([]byte, 1)
			if _, err := io.ReadFull(conn, pidBuf); err != nil {
				break
			}

			if pidBuf[0] == 0x10 { // Client ExtInfo
				extInfo := make([]byte, 66)
				io.ReadFull(conn, extInfo)
				expectedEntries = int(binary.BigEndian.Uint16(extInfo[64:66]))
				if expectedEntries == 0 {
					break
				}
			} else if pidBuf[0] == 0x11 { // Client ExtEntry
				extEntry := make([]byte, 68)
				io.ReadFull(conn, extEntry)
				extName := strings.TrimRight(string(extEntry[0:64]), " ")
				switch extName {
				case "CustomBlocks":
					clientSupportsCustomBlocks = true
				case "BlockDefinitions":
					clientSupportsBlockDefs = true
				case "HeldBlock":
					clientSupportsHeldBlock = true
				case "EnvMapAspect":
					clientSupportsTexturePack = true
				}
				entriesRead++
			} else {
				break
			}
		}
		conn.SetReadDeadline(time.Time{})

		// CustomBlocks level exchange — opcode is 0x13 per CPE spec
		if clientSupportsCustomBlocks {
			writeUint8(conn, 0x13)
			writeUint8(conn, 1) // Support Level 1

			// Read the client's 0x13 reply
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			pidBuf := make([]byte, 1)
			if _, err := io.ReadFull(conn, pidBuf); err == nil && pidBuf[0] == 0x13 {
				io.ReadFull(conn, make([]byte, 1))
			}
			conn.SetReadDeadline(time.Time{})
		}
	}

	player := &Player{
		Conn:                conn,
		Username:            username,
		Server:              server,
		Authenticated:       make(map[string]bool),
		SavedPasswords:      make(map[string]string),
		SupportsCPE:         clientSupportsCustomBlocks,
		SupportsBlockDefs:   clientSupportsBlockDefs,
		SupportsHeldBlock:   clientSupportsHeldBlock,
		SupportsTexturePack: clientSupportsTexturePack,
	}

	// Push all custom block definitions BEFORE the first map is sent
	if clientSupportsBlockDefs {
		sendAllCustomBlocks(conn)
	}

	// Enforce max player limit and allocate a unique ID
	server.mu.Lock()
	if len(server.players) >= serverConfig.MaxPlayers {
		server.mu.Unlock()
		disconnectPlayer(conn, "&cServer is full!")
		return
	}
	player.ID = server.allocateID()
	server.players[username] = player
	server.mu.Unlock()

	defer func() {
		// Despawn from all players in the same world
		server.despawnPlayerFromWorld(player)

		server.mu.Lock()
		server.freeID(player.ID)
		delete(server.players, username)
		server.mu.Unlock()

		if player.World != nil {
			for _, pl := range server.plugins {
				pl.OnPlayerLeave(player, player.World)
			}
		}
	}()

	// Load saved player state from SQLite
	savedWorld, sx, sy, sz, sYaw, sPitch, sHeld, savedPws, hasSaved := loadPlayerState(username)

	worldSet := false
	if hasSaved {
		for k, v := range savedPws {
			player.SavedPasswords[k] = v
		}
		player.HeldBlock = sHeld
		if w, ok := server.GetWorld(savedWorld); ok {
			player.X, player.Y, player.Z = sx, sy, sz
			player.Yaw, player.Pitch = sYaw, sPitch
			player.ChangeWorld(w, true)
			worldSet = true
		}
	}

	if !worldSet {
		if hub, ok := server.GetWorld("hub"); ok {
			player.ChangeWorld(hub, false)
		}
	}

	// Restore held block after the world is loaded
	if hasSaved && player.SupportsHeldBlock && sHeld > 0 {
		sendSetHeldBlock(player.Conn, sHeld, 0)
	}

	idBuf := make([]byte, 1)
	for {
		if _, err := io.ReadFull(conn, idBuf); err != nil {
			break
		}
		pid := idBuf[0]

		size, known := clientPacketSizes[pid]
		if !known {
			break
		}

		data := make([]byte, size)
		if _, err := io.ReadFull(conn, data); err != nil {
			break
		}

		switch pid {
		case 0x05:
			x := int16(binary.BigEndian.Uint16(data[0:2]))
			y := int16(binary.BigEndian.Uint16(data[2:4]))
			z := int16(binary.BigEndian.Uint16(data[4:6]))
			mode, block := data[6], data[7]

			// Track the block in hand (when placing, not breaking)
			if mode != 0 {
				player.mu.Lock()
				player.HeldBlock = block
				player.mu.Unlock()
			}

			// ── Inspect mode: show block history instead of building ──
			if player.InspectMode && player.World != nil {
				// Revert the client's visual change
				sendBlockUpdate(conn, x, y, z, player.World.GetBlock(int(x), int(y), int(z)), player.SupportsCPE)

				history := getBlockHistoryAt(player.World.Name, x, y, z, 8)
				if len(history) == 0 {
					curBlock := player.World.GetBlock(int(x), int(y), int(z))
					player.SendMessage(fmt.Sprintf("&e(%d,%d,%d) &f%s&7: No recorded changes.", x, y, z, blockName(curBlock)))
				} else {
					curBlock := player.World.GetBlock(int(x), int(y), int(z))
					player.SendMessage(fmt.Sprintf("&e--- (%d,%d,%d) %s ---", x, y, z, blockName(curBlock)))
					for _, c := range history {
						player.SendMessage(fmt.Sprintf("  &b%s &7%s &f%s &7→ &f%s",
							c.Player, formatTimeAgo(c.Time), blockName(c.OldBlock), blockName(c.NewBlock)))
					}
				}
				continue
			}

			if mode != 0 {
				isBanned := false
				for _, b := range serverConfig.BannedBlocks {
					if block == b {
						isBanned = true
						break
					}
				}
				if isBanned {
					player.SendMessage("&cThat block is banned on this server.")
					if player.World != nil {
						sendBlockUpdate(conn, x, y, z, player.World.GetBlock(int(x), int(y), int(z)), player.SupportsCPE)
					}
					continue
				}
			}

			// Capture old block before applying change
			var oldBlock byte
			if player.World != nil {
				oldBlock = player.World.GetBlock(int(x), int(y), int(z))
			}

			if mode == 0 {
				block = 0
			}

			cancel := false
			for _, pl := range server.plugins {
				if pl.OnBlockChange(player, x, y, z, block) == EventCancel {
					cancel = true
					break
				}
			}

			if !cancel && player.World != nil {
				player.World.SetBlock(int(x), int(y), int(z), block)

				// Record the change
				recordBlockChange(player.World.Name, player.Username, x, y, z, oldBlock, block)

				server.mu.RLock()
				for _, p2 := range server.players {
					if p2.World == player.World && p2 != player {
						sendBlockUpdate(p2.Conn, x, y, z, block, p2.SupportsCPE)
					}
				}
				server.mu.RUnlock()
			}

		case 0x08:
			player.X = int16(binary.BigEndian.Uint16(data[1:3]))
			player.Y = int16(binary.BigEndian.Uint16(data[3:5]))
			player.Z = int16(binary.BigEndian.Uint16(data[5:7]))
			player.Yaw, player.Pitch = data[7], data[8]

			// Broadcast our position to all other players in the same world
			server.broadcastPosition(player)

			for _, pl := range server.plugins {
				pl.OnPlayerMove(player, player.X, player.Y, player.Z, player.Yaw, player.Pitch)
			}
			updatePlayerState(player)

		case 0x0D:
			msg := strings.TrimRight(string(data[1:65]), " ")

			// ── /op ──
			if strings.HasPrefix(msg, "/op ") {
				password := strings.TrimPrefix(msg, "/op ")
				if serverConfig.OpPassword != "" && hashPassword(password) == serverConfig.OpPassword {
					player.IsOp = true
					player.SendMessage("&aYou are now a server operator.")
				} else {
					player.SendMessage("&cIncorrect operator password.")
				}
				continue
			}

			// ── /pass ──
			if strings.HasPrefix(msg, "/pass ") {
				password := strings.TrimPrefix(msg, "/pass ")
				player.mu.Lock()
				pending := player.PendingWorld
				player.mu.Unlock()

				if pending != nil && serverConfig.WorldPasswords[pending.Name] == hashPassword(password) {
					player.mu.Lock()
					player.Authenticated[pending.Name] = true
					player.SavedPasswords[pending.Name] = hashPassword(password)
					player.PendingWorld = nil
					player.mu.Unlock()
					updatePlayerState(player)
					player.SendMessage("&aPassword accepted!")
					player.ChangeWorld(pending, false)
				} else {
					player.SendMessage("&cIncorrect password or no world pending.")
				}
				continue
			}

			// ── /set ──
			if strings.HasPrefix(msg, "/set ") {
				if !player.IsOp {
					player.SendMessage("&cYou must be an operator to use /set.")
					continue
				}
				parts := strings.Split(msg, " ")
				if len(parts) >= 3 && parts[1] == "op" {
					serverConfig.OpPassword = hashPassword(parts[2])
					saveConfig()
					player.SendMessage("&eOperator password securely updated.")
				} else if len(parts) >= 4 && parts[1] == "world" {
					worldName := parts[2]
					serverConfig.WorldPasswords[worldName] = hashPassword(parts[3])
					saveConfig()
					player.SendMessage(fmt.Sprintf("&ePassword for world '%s' securely updated.", worldName))
				} else {
					player.SendMessage("&cUsage: /set op <pass> OR /set world <n> <pass>")
				}
				continue
			}

			// ── /bg — Custom Block Management (op only) ──
			if msg == "/bg" || strings.HasPrefix(msg, "/bg ") {
				if !player.IsOp {
					player.SendMessage("&cYou must be an operator to use /bg.")
					continue
				}
				handleBlockCommand(player, server, msg)
				continue
			}

			// ── /hand <id> [lock] — Set the block in your hand ──
			if strings.HasPrefix(msg, "/hand ") {
				parts := strings.Fields(msg)
				if len(parts) < 2 {
					player.SendMessage("&cUsage: /hand <blockID> [lock]")
					continue
				}
				id, err := strconv.Atoi(parts[1])
				if err != nil || id < 0 || id > 255 {
					player.SendMessage("&cBlock ID must be 0-255.")
					continue
				}
				if !player.SupportsHeldBlock {
					player.SendMessage("&cYour client does not support the HeldBlock extension.")
					continue
				}
				lock := byte(0)
				if len(parts) >= 3 && (parts[2] == "lock" || parts[2] == "1") {
					lock = 1
				}
				sendSetHeldBlock(player.Conn, byte(id), lock)
				player.mu.Lock()
				player.HeldBlock = byte(id)
				player.mu.Unlock()
				updatePlayerState(player)
				lockStr := ""
				if lock == 1 {
					lockStr = " &7(locked)"
				}
				player.SendMessage(fmt.Sprintf("&aHeld block: &f%s &7(%d)%s", blockName(byte(id)), id, lockStr))
				continue
			}

			// ── /kick <player> [reason] ──
			if strings.HasPrefix(msg, "/kick ") {
				if !player.IsOp {
					player.SendMessage("&cYou must be an operator to use /kick.")
					continue
				}
				parts := strings.SplitN(msg, " ", 3)
				if len(parts) < 2 {
					player.SendMessage("&cUsage: /kick <player> [reason]")
					continue
				}
				targetName := parts[1]
				reason := "Kicked by operator"
				if len(parts) >= 3 {
					reason = parts[2]
				}

				server.mu.RLock()
				target, found := server.players[targetName]
				server.mu.RUnlock()

				if !found {
					player.SendMessage("&cPlayer not found: &e" + targetName)
				} else {
					disconnectPlayer(target.Conn, "&e"+reason)
					target.Conn.Close()
					player.SendMessage("&aKicked &f" + targetName + "&a: " + reason)
					log.Printf("[Kick] %s kicked %s: %s", player.Username, targetName, reason)
				}
				continue
			}

			// ── /ban <player> [reason] ──
			if strings.HasPrefix(msg, "/ban ") {
				if !player.IsOp {
					player.SendMessage("&cYou must be an operator to use /ban.")
					continue
				}
				parts := strings.SplitN(msg, " ", 3)
				if len(parts) < 2 {
					player.SendMessage("&cUsage: /ban <player> [reason]")
					continue
				}
				targetName := parts[1]
				reason := ""
				if len(parts) >= 3 {
					reason = parts[2]
				}

				lowerName := strings.ToLower(targetName)
				if _, already := isPlayerBanned(targetName); already {
					player.SendMessage("&e" + targetName + " is already banned.")
					continue
				}

				// Get the target's IP if they're online
				targetIP := ""
				server.mu.RLock()
				if target, found := server.players[targetName]; found {
					if addr, ok := target.Conn.RemoteAddr().(*net.TCPAddr); ok {
						targetIP = addr.IP.String()
					}
				}
				server.mu.RUnlock()

				banMutex.Lock()
				bannedPlayers[lowerName] = BanEntry{
					Username: targetName,
					Reason:   reason,
					BannedBy: player.Username,
					Time:     time.Now().Format(time.RFC3339),
					IP:       targetIP,
				}
				banMutex.Unlock()
				saveBanList()

				// Kick them if online
				server.mu.RLock()
				target, found := server.players[targetName]
				server.mu.RUnlock()
				if found {
					kickMsg := "&cBanned"
					if reason != "" {
						kickMsg = "&cBanned: " + reason
					}
					disconnectPlayer(target.Conn, kickMsg)
					target.Conn.Close()
				}

				banMsg := "&aBanned &f" + targetName
				if reason != "" {
					banMsg += "&a: " + reason
				}
				player.SendMessage(banMsg)
				log.Printf("[Ban] %s banned %s: %s", player.Username, targetName, reason)
				continue
			}

			// ── /unban <player> ──
			if strings.HasPrefix(msg, "/unban ") {
				if !player.IsOp {
					player.SendMessage("&cYou must be an operator to use /unban.")
					continue
				}
				targetName := strings.TrimPrefix(msg, "/unban ")
				targetName = strings.TrimSpace(targetName)

				lowerName := strings.ToLower(targetName)
				banMutex.Lock()
				_, existed := bannedPlayers[lowerName]
				if existed {
					delete(bannedPlayers, lowerName)
				}
				banMutex.Unlock()

				if existed {
					saveBanList()
					player.SendMessage("&aUnbanned &f" + targetName)
					log.Printf("[Ban] %s unbanned %s", player.Username, targetName)
				} else {
					player.SendMessage("&e" + targetName + " is not banned.")
				}
				continue
			}

			// ── /banlist ──
			if msg == "/banlist" {
				if !player.IsOp {
					player.SendMessage("&cYou must be an operator to use /banlist.")
					continue
				}
				banMutex.RLock()
				count := len(bannedPlayers)
				if count == 0 {
					banMutex.RUnlock()
					player.SendMessage("&eNo players are banned.")
					continue
				}
				player.SendMessage(fmt.Sprintf("&e--- Banned players (%d) ---", count))
				for _, b := range bannedPlayers {
					line := fmt.Sprintf("  &c%s", b.Username)
					if b.Reason != "" {
						line += " &7— " + b.Reason
					}
					if b.BannedBy != "" {
						line += " &8(by " + b.BannedBy + ")"
					}
					player.SendMessage(line)
				}
				banMutex.RUnlock()
				continue
			}

			// ── /bi — Toggle block inspect mode (op only) ──
			if msg == "/bi" {
				if !player.IsOp {
					player.SendMessage("&cYou must be an operator to use /bi.")
					continue
				}
				player.InspectMode = !player.InspectMode
				if player.InspectMode {
					player.SendMessage("&aBlock inspect ON &7— click any block to see its history.")
					player.SendMessage("&7Type /bi again to turn off.")
				} else {
					player.SendMessage("&eBlock inspect OFF.")
				}
				continue
			}

			// ── /undo <player> [seconds] — Revert a player's changes ──
			if strings.HasPrefix(msg, "/undo ") {
				if !player.IsOp {
					player.SendMessage("&cYou must be an operator to use /undo.")
					continue
				}
				if player.World == nil {
					player.SendMessage("&cYou must be in a world.")
					continue
				}
				parts := strings.Fields(msg)
				if len(parts) < 2 {
					player.SendMessage("&cUsage: /undo <player> [seconds]")
					continue
				}
				targetName := parts[1]
				seconds := int64(300) // default 5 minutes
				if len(parts) >= 3 {
					if n, err := strconv.ParseInt(parts[2], 10, 64); err == nil && n > 0 {
						seconds = n
					}
				}
				since := time.Now().Unix() - seconds

				changes := getPlayerChanges(player.World.Name, targetName, since)
				if len(changes) == 0 {
					player.SendMessage(fmt.Sprintf("&eNo changes by %s in the last %ds.", targetName, seconds))
					continue
				}

				// Apply reverts — changes are already newest-first
				reverted := 0
				for _, c := range changes {
					player.World.SetBlock(int(c.X), int(c.Y), int(c.Z), c.OldBlock)
					recordBlockChange(player.World.Name, "[undo:"+player.Username+"]", c.X, c.Y, c.Z, c.NewBlock, c.OldBlock)

					// Broadcast the revert to all players in the world
					server.mu.RLock()
					for _, p2 := range server.players {
						if p2.World == player.World {
							sendBlockUpdate(p2.Conn, c.X, c.Y, c.Z, c.OldBlock, p2.SupportsCPE)
						}
					}
					server.mu.RUnlock()
					reverted++
				}

				player.SendMessage(fmt.Sprintf("&aReverted %d change(s) by %s (last %ds).", reverted, targetName, seconds))
				log.Printf("[Undo] %s reverted %d changes by %s in %q", player.Username, reverted, targetName, player.World.Name)
				continue
			}

			// ── /purgehistory [days] — Clear old history (op only) ──
			if strings.HasPrefix(msg, "/purgehistory") {
				if !player.IsOp {
					player.SendMessage("&cYou must be an operator to use /purgehistory.")
					continue
				}
				days := 7 // default
				parts := strings.Fields(msg)
				if len(parts) >= 2 {
					if n, err := strconv.Atoi(parts[1]); err == nil && n > 0 {
						days = n
					}
				}
				pruneBlockHistory(time.Duration(days) * 24 * time.Hour)
				player.SendMessage(fmt.Sprintf("&aPurged history older than %d day(s).", days))
				continue
			}

			// ── /pixelart <url> [maxsize] — Generate pixel art from image ──
			if strings.HasPrefix(msg, "/pixelart ") {
				if !player.IsOp {
					player.SendMessage("&cYou must be an operator to use /pixelart.")
					continue
				}
				parts := strings.Fields(msg)
				if len(parts) < 2 {
					player.SendMessage("&cUsage: /pixelart <url> [maxsize]")
					player.SendMessage("&7Faces the direction you're looking. Max size default: 64")
					continue
				}
				imageURL := parts[1]
				maxSize := 64
				if len(parts) >= 3 {
					if n, err := strconv.Atoi(parts[2]); err == nil && n > 0 && n <= 256 {
						maxSize = n
					}
				}
				// Run in background so it doesn't block the player's connection
				go placePixelArt(player, server, imageURL, maxSize)
				continue
			}

			// ── /goto ──
			if strings.HasPrefix(msg, "/goto ") {
				name := strings.TrimPrefix(msg, "/goto ")
				if w, ok := server.GetWorld(name); ok {
					player.ChangeWorld(w, false)
				} else {
					player.SendMessage("&cWorld not found: &e" + name)
				}
				continue
			}

			// ── /newlvl ──
			if strings.HasPrefix(msg, "/newlvl ") {
				if !player.IsOp {
					player.SendMessage("&cYou must be a server op to use this command.")
					continue
				}
				parts := strings.Split(msg, " ")
				if len(parts) == 5 {
					name := parts[1]
					w, _ := strconv.Atoi(parts[2])
					h, _ := strconv.Atoi(parts[3])
					l, _ := strconv.Atoi(parts[4])

					newWorld := generateFlatWorld(int16(w), int16(h), int16(l))
					server.AddWorld(name, newWorld)
					SaveMCGalaxyLevel(newWorld, "levels/"+name+".lvl")
					player.SendMessage("&aCreated new level: " + name)
				} else {
					player.SendMessage("&cUsage: /newlvl name width height length")
				}
				continue
			}

			// ── /resizelvl ──
			if strings.HasPrefix(msg, "/resizelvl ") {
				if !player.IsOp {
					player.SendMessage("&cYou must be a server op to use this command.")
					continue
				}
				parts := strings.Split(msg, " ")
				if len(parts) == 4 && player.World != nil {
					newW, _ := strconv.Atoi(parts[1])
					newH, _ := strconv.Atoi(parts[2])
					newL, _ := strconv.Atoi(parts[3])

					oldWorld := player.World
					oldWorld.mu.Lock()

					newBlocks := make([]byte, newW*newH*newL)
					for y := 0; y < int(oldWorld.Height) && y < newH; y++ {
						for z := 0; z < int(oldWorld.Length) && z < newL; z++ {
							for x := 0; x < int(oldWorld.Width) && x < newW; x++ {
								oldIdx := y*int(oldWorld.Width)*int(oldWorld.Length) + z*int(oldWorld.Width) + x
								newIdx := y*newW*newL + z*newW + x
								newBlocks[newIdx] = oldWorld.Blocks[oldIdx]
							}
						}
					}

					oldWorld.Width = int16(newW)
					oldWorld.Height = int16(newH)
					oldWorld.Length = int16(newL)
					oldWorld.Blocks = newBlocks
					oldWorld.mu.Unlock()

					server.mu.RLock()
					for _, p2 := range server.players {
						if p2.World == oldWorld {
							sendMapToPlayer(p2.Conn, oldWorld, p2.SupportsCPE)
							p2.SendMessage("&eMap resized by " + username)
						}
					}
					server.mu.RUnlock()
					SaveMCGalaxyLevel(oldWorld, "levels/"+oldWorld.Name+".lvl")
				} else {
					player.SendMessage("&cUsage: /resizelvl width height length")
				}
				continue
			}

			// ── chat ──
			cancel := false
			for _, pl := range server.plugins {
				if pl.OnChat(player, msg) == EventCancel {
					cancel = true
					break
				}
			}
			if !cancel {
				server.mu.RLock()
				for _, p2 := range server.players {
					if p2.World == player.World {
						p2.SendMessage("&f" + username + ": &7" + msg)
					}
				}
				server.mu.RUnlock()
			}
		}
	}
}

// ═══════════════════════════════════════════════════════════════════
// /bg  COMMAND  HANDLER  —  Custom Block Definitions
// ═══════════════════════════════════════════════════════════════════

var bgPropertyHelp = map[string]string{
	"name":      "Block display name (can contain spaces)",
	"solidity":  "0=walk-through 1=swim-through 2=solid",
	"speed":     "1=0.25x 2=0.5x 3=0.75x 4=1.0x 5=1.25x 6=1.5x 7=1.75x 8=2.0x",
	"toptex":    "Texture atlas index for top face",
	"sidetex":   "Texture atlas index for all side faces",
	"bottomtex": "Texture atlas index for bottom face",
	"alltex":    "Set top+side+bottom textures at once",
	"light":     "0=blocks light 1=transmits light",
	"sound":     "0=none 1=wood 2=gravel 3=grass 4=stone 5=metal 6=glass 7=wool 8=sand 9=snow",
	"bright":    "0=normal 1=full bright (glows)",
	"shape":     "0=sprite 1-16=height in sixteenths (16=full block)",
	"draw":      "0=opaque 1=transparent(same) 2=transparent(diff) 3=translucent 4=gas",
	"fog":       "density r g b  (each 0-255)",
	"fallback":  "Vanilla block ID for non-CPE clients (0-49)",
}

func handleBlockCommand(player *Player, server *Server, msg string) {
	parts := strings.Fields(msg)
	if len(parts) < 2 {
		player.SendMessage("&e--- Custom Block commands ---")
		player.SendMessage("&b/bg define <id> <name> &7- create (50-255)")
		player.SendMessage("&b/bg set <id> <prop> <val> &7- edit property")
		player.SendMessage("&b/bg remove <id> &7- delete definition")
		player.SendMessage("&b/bg list &7- show all custom blocks")
		player.SendMessage("&b/bg info <id> &7- show block details")
		player.SendMessage("&b/bg props &7- list editable properties")
		player.SendMessage("&b/bg copy <src> <dst> &7- duplicate a block")
		player.SendMessage("&b/bg import [path] &7- load MCGalaxy global.json")
		player.SendMessage("&b/bg export [path] &7- save as MCGalaxy format")
		return
	}

	switch parts[1] {

	// ── /bg define <id> <name…> ──────────────────────────────
	case "define":
		if len(parts) < 4 {
			player.SendMessage("&cUsage: /bg define <id 50-255> <name>")
			return
		}
		id, err := strconv.Atoi(parts[2])
		if err != nil || id < 50 || id > 255 {
			player.SendMessage("&cBlock ID must be between 50 and 255.")
			return
		}
		name := strings.Join(parts[3:], " ")
		if len(name) > 64 {
			name = name[:64]
		}

		customBlocksMu.RLock()
		_, exists := customBlocks[byte(id)]
		customBlocksMu.RUnlock()
		if exists {
			player.SendMessage(fmt.Sprintf("&cBlock %d already exists. /bg remove %d first.", id, id))
			return
		}

		cb := CustomBlock{
			ID:            byte(id),
			Name:          name,
			Solidity:      2, // solid
			Speed:         4, // normal speed
			TopTex:        1, // stone texture
			SideTex:       1,
			BottomTex:     1,
			TransmitLight: 0,
			WalkSound:     4, // stone sound
			FullBright:    0,
			Shape:         16, // full block
			BlockDraw:     0,  // opaque
			FogDensity:    0,
			FogR:          0,
			FogG:          0,
			FogB:          0,
			Fallback:      1, // stone
		}

		customBlocksMu.Lock()
		customBlocks[byte(id)] = cb
		customBlocksMu.Unlock()
		saveCustomBlocks()
		server.broadcastDefineBlock(cb)

		player.SendMessage(fmt.Sprintf("&aBlock %d &f(%s)&a defined!", id, name))
		player.SendMessage("&7Use &b/bg set " + parts[2] + " <prop> <val>&7 to customise it.")

	// ── /bg set <id> <property> <value…> ─────────────────────
	case "set":
		if len(parts) < 5 {
			player.SendMessage("&cUsage: /bg set <id> <property> <value>")
			player.SendMessage("&7Type &b/bg props&7 for the property list.")
			return
		}
		id, err := strconv.Atoi(parts[2])
		if err != nil || id < 50 || id > 255 {
			player.SendMessage("&cBlock ID must be between 50 and 255.")
			return
		}

		customBlocksMu.RLock()
		cb, exists := customBlocks[byte(id)]
		customBlocksMu.RUnlock()
		if !exists {
			player.SendMessage(fmt.Sprintf("&cBlock %d is not defined.", id))
			return
		}

		prop := strings.ToLower(parts[3])
		valStr := parts[4]
		parseByte := func() byte { n, _ := strconv.Atoi(valStr); return byte(n) }

		switch prop {
		case "name":
			cb.Name = strings.Join(parts[4:], " ")
			if len(cb.Name) > 64 {
				cb.Name = cb.Name[:64]
			}
		case "solidity":
			v := parseByte()
			if v > 2 {
				player.SendMessage("&c" + bgPropertyHelp["solidity"])
				return
			}
			cb.Solidity = v
		case "speed":
			v := parseByte()
			if v < 1 || v > 8 {
				player.SendMessage("&c" + bgPropertyHelp["speed"])
				return
			}
			cb.Speed = v
		case "toptex":
			cb.TopTex = parseByte()
		case "sidetex":
			cb.SideTex = parseByte()
		case "bottomtex":
			cb.BottomTex = parseByte()
		case "alltex":
			t := parseByte()
			cb.TopTex = t
			cb.SideTex = t
			cb.BottomTex = t
		case "light":
			v := parseByte()
			if v > 1 {
				player.SendMessage("&c" + bgPropertyHelp["light"])
				return
			}
			cb.TransmitLight = v
		case "sound":
			v := parseByte()
			if v > 9 {
				player.SendMessage("&c" + bgPropertyHelp["sound"])
				return
			}
			cb.WalkSound = v
		case "bright":
			v := parseByte()
			if v > 1 {
				player.SendMessage("&c" + bgPropertyHelp["bright"])
				return
			}
			cb.FullBright = v
		case "shape":
			v := parseByte()
			if v > 16 {
				player.SendMessage("&c" + bgPropertyHelp["shape"])
				return
			}
			cb.Shape = v
		case "draw":
			v := parseByte()
			if v > 4 {
				player.SendMessage("&c" + bgPropertyHelp["draw"])
				return
			}
			cb.BlockDraw = v
		case "fog":
			if len(parts) < 8 {
				player.SendMessage("&cUsage: /bg set <id> fog <density> <r> <g> <b>")
				return
			}
			d, _ := strconv.Atoi(parts[4])
			r, _ := strconv.Atoi(parts[5])
			g, _ := strconv.Atoi(parts[6])
			bl, _ := strconv.Atoi(parts[7])
			cb.FogDensity = byte(d)
			cb.FogR = byte(r)
			cb.FogG = byte(g)
			cb.FogB = byte(bl)
		case "fallback":
			v := parseByte()
			if v > 49 {
				player.SendMessage("&cFallback must be a vanilla block (0-49).")
				return
			}
			cb.Fallback = v
		default:
			player.SendMessage("&cUnknown property: &e" + prop)
			player.SendMessage("&7Type &b/bg props&7 for the full list.")
			return
		}

		customBlocksMu.Lock()
		customBlocks[byte(id)] = clampBlockProperties(cb)
		customBlocksMu.Unlock()
		saveCustomBlocks()
		server.broadcastDefineBlock(cb)

		player.SendMessage(fmt.Sprintf("&aBlock %d: %s = %s", id, prop, strings.Join(parts[4:], " ")))

	// ── /bg remove <id> ──────────────────────────────────────
	case "remove":
		if len(parts) < 3 {
			player.SendMessage("&cUsage: /bg remove <id>")
			return
		}
		id, err := strconv.Atoi(parts[2])
		if err != nil || id < 50 || id > 255 {
			player.SendMessage("&cBlock ID must be between 50 and 255.")
			return
		}

		customBlocksMu.Lock()
		_, exists := customBlocks[byte(id)]
		if exists {
			delete(customBlocks, byte(id))
		}
		customBlocksMu.Unlock()

		if !exists {
			player.SendMessage(fmt.Sprintf("&cBlock %d is not defined.", id))
			return
		}

		saveCustomBlocks()
		server.broadcastRemoveBlock(byte(id))
		player.SendMessage(fmt.Sprintf("&aBlock %d removed.", id))

	// ── /bg list ─────────────────────────────────────────────
	case "list":
		customBlocksMu.RLock()
		count := len(customBlocks)
		if count == 0 {
			customBlocksMu.RUnlock()
			player.SendMessage("&eNo custom blocks defined yet.")
			player.SendMessage("&7Use &b/bg define <id> <name>&7 to create one.")
			return
		}
		player.SendMessage(fmt.Sprintf("&e--- Custom Blocks (%d) ---", count))
		for _, cb := range customBlocks {
			player.SendMessage(fmt.Sprintf("  &b%d &f%s &7tex:%d/%d/%d fb:%d",
				cb.ID, cb.Name, cb.TopTex, cb.SideTex, cb.BottomTex, cb.Fallback))
		}
		customBlocksMu.RUnlock()

	// ── /bg info <id> ────────────────────────────────────────
	case "info":
		if len(parts) < 3 {
			player.SendMessage("&cUsage: /bg info <id>")
			return
		}
		id, err := strconv.Atoi(parts[2])
		if err != nil || id < 50 || id > 255 {
			player.SendMessage("&cBlock ID must be between 50 and 255.")
			return
		}
		customBlocksMu.RLock()
		cb, exists := customBlocks[byte(id)]
		customBlocksMu.RUnlock()
		if !exists {
			player.SendMessage(fmt.Sprintf("&cBlock %d is not defined.", id))
			return
		}
		player.SendMessage(fmt.Sprintf("&e--- Block %d: &f%s &e---", cb.ID, cb.Name))
		player.SendMessage(fmt.Sprintf("  &7solidity=&f%d &7speed=&f%d &7shape=&f%d &7draw=&f%d", cb.Solidity, cb.Speed, cb.Shape, cb.BlockDraw))
		player.SendMessage(fmt.Sprintf("  &7toptex=&f%d &7sidetex=&f%d &7bottomtex=&f%d", cb.TopTex, cb.SideTex, cb.BottomTex))
		player.SendMessage(fmt.Sprintf("  &7light=&f%d &7sound=&f%d &7bright=&f%d", cb.TransmitLight, cb.WalkSound, cb.FullBright))
		player.SendMessage(fmt.Sprintf("  &7fog=&f%d,%d,%d,%d &7fallback=&f%d", cb.FogDensity, cb.FogR, cb.FogG, cb.FogB, cb.Fallback))

	// ── /bg copy <src> <dst> ─────────────────────────────────
	case "copy":
		if len(parts) < 4 {
			player.SendMessage("&cUsage: /bg copy <srcID> <dstID>")
			return
		}
		srcID, err1 := strconv.Atoi(parts[2])
		dstID, err2 := strconv.Atoi(parts[3])
		if err1 != nil || err2 != nil || srcID < 50 || srcID > 255 || dstID < 50 || dstID > 255 {
			player.SendMessage("&cBoth IDs must be between 50 and 255.")
			return
		}
		customBlocksMu.RLock()
		src, srcExists := customBlocks[byte(srcID)]
		_, dstExists := customBlocks[byte(dstID)]
		customBlocksMu.RUnlock()
		if !srcExists {
			player.SendMessage(fmt.Sprintf("&cSource block %d is not defined.", srcID))
			return
		}
		if dstExists {
			player.SendMessage(fmt.Sprintf("&cDestination block %d already exists. /bg remove %d first.", dstID, dstID))
			return
		}
		dst := src
		dst.ID = byte(dstID)
		dst.Name = src.Name + " (copy)"
		if len(dst.Name) > 64 {
			dst.Name = dst.Name[:64]
		}

		customBlocksMu.Lock()
		customBlocks[byte(dstID)] = dst
		customBlocksMu.Unlock()
		saveCustomBlocks()
		server.broadcastDefineBlock(dst)

		player.SendMessage(fmt.Sprintf("&aBlock %d copied to %d.", srcID, dstID))

	// ── /bg props ────────────────────────────────────────────
	case "props":
		player.SendMessage("&e--- Editable properties ---")
		for _, kv := range []struct{ k, v string }{
			{"name", bgPropertyHelp["name"]},
			{"solidity", bgPropertyHelp["solidity"]},
			{"speed", bgPropertyHelp["speed"]},
			{"toptex", bgPropertyHelp["toptex"]},
			{"sidetex", bgPropertyHelp["sidetex"]},
			{"bottomtex", bgPropertyHelp["bottomtex"]},
			{"alltex", bgPropertyHelp["alltex"]},
			{"light", bgPropertyHelp["light"]},
			{"sound", bgPropertyHelp["sound"]},
			{"bright", bgPropertyHelp["bright"]},
			{"shape", bgPropertyHelp["shape"]},
			{"draw", bgPropertyHelp["draw"]},
			{"fog", bgPropertyHelp["fog"]},
			{"fallback", bgPropertyHelp["fallback"]},
		} {
			player.SendMessage(fmt.Sprintf("  &b%-9s &7%s", kv.k, kv.v))
		}

	// ── /bg import [path] ────────────────────────────────────
	case "import":
		path := "global.json"
		if len(parts) >= 3 {
			path = strings.Join(parts[2:], " ")
		}
		player.SendMessage("&eImporting MCGalaxy blocks from &f" + path + "&e...")
		imported, skipped, err := importMCGalaxyBlocks(path)
		if err != nil {
			player.SendMessage("&cImport failed: " + err.Error())
			return
		}
		if imported > 0 {
			saveCustomBlocks()
			// Push all definitions to connected BlockDefs clients
			customBlocksMu.RLock()
			for _, cb := range customBlocks {
				server.broadcastDefineBlock(cb)
			}
			customBlocksMu.RUnlock()
		}
		player.SendMessage(fmt.Sprintf("&aImported %d block(s), skipped %d.", imported, skipped))

	// ── /bg export [path] ────────────────────────────────────
	case "export":
		path := "global_export.json"
		if len(parts) >= 3 {
			path = strings.Join(parts[2:], " ")
		}
		customBlocksMu.RLock()
		var out []mcgBlockDef
		for _, cb := range customBlocks {
			blocksLight := cb.TransmitLight == 0
			fullBright := cb.FullBright == 1
			out = append(out, mcgBlockDef{
				BlockID:     int(cb.ID),
				Name:        cb.Name,
				Speed:       int(cb.Speed),
				CollideType: int(cb.Solidity),
				TopTex:      int(cb.TopTex),
				BottomTex:   int(cb.BottomTex),
				LeftTex:     int(cb.SideTex),
				RightTex:    int(cb.SideTex),
				FrontTex:    int(cb.SideTex),
				BackTex:     int(cb.SideTex),
				BlocksLight: blocksLight,
				WalkSound:   int(cb.WalkSound),
				FullBright:  fullBright,
				Shape:       int(cb.Shape),
				BlockDraw:   int(cb.BlockDraw),
				FallBack:    int(cb.Fallback),
				FogDensity:  int(cb.FogDensity),
				FogR:        int(cb.FogR),
				FogG:        int(cb.FogG),
				FogB:        int(cb.FogB),
				MinX:        0, MinY: 0, MinZ: 0,
				MaxX: 16, MaxY: 16, MaxZ: 16,
			})
		}
		customBlocksMu.RUnlock()
		data, _ := json.MarshalIndent(out, "", "    ")
		if err := os.WriteFile(path, data, 0644); err != nil {
			player.SendMessage("&cExport failed: " + err.Error())
			return
		}
		player.SendMessage(fmt.Sprintf("&aExported %d block(s) to &f%s", len(out), path))

	default:
		player.SendMessage("&cUnknown subcommand. Type &b/bg&c for help.")
	}
}

// ═══════════════════════════════════════════════════════════════════
// BUILT-IN PLUGINS
// ═══════════════════════════════════════════════════════════════════

type HubProtectionPlugin struct{ BasePlugin }

func (h *HubProtectionPlugin) Name() string { return "HubProtection" }
func (h *HubProtectionPlugin) OnBlockChange(p *Player, x, y, z int16, id byte) EventResult {
	if p.World.Name == "hub" {
		if x >= 27 && x <= 36 && y >= 15 && y <= 21 && z >= 27 && z <= 36 {
			p.SendMessage("&cSpawn building is protected!")
			sendBlockUpdate(p.Conn, x, y, z, p.World.GetBlock(int(x), int(y), int(z)), p.SupportsCPE)
			return EventCancel
		}
	}
	return EventContinue
}

type PortalPlugin struct {
	BasePlugin
	cooldowns map[string]time.Time
	mu        sync.Mutex
}

func NewPortalPlugin() *PortalPlugin  { return &PortalPlugin{cooldowns: make(map[string]time.Time)} }
func (pp *PortalPlugin) Name() string { return "Portals" }
func (pp *PortalPlugin) OnPlayerMove(player *Player, x, y, z int16, yaw, pitch byte) {
	pp.mu.Lock()
	last := pp.cooldowns[player.Username]
	pp.mu.Unlock()

	if time.Since(last) < 2*time.Second || player.World == nil {
		return
	}
	bx, by, bz := x/32, y/32, z/32

	for _, portal := range player.World.Portals {
		if bx >= portal.MinX && bx <= portal.MaxX && by >= portal.MinY && by <= portal.MaxY && bz >= portal.MinZ && bz <= portal.MaxZ {
			target, ok := player.Server.GetWorld(portal.TargetWorld)
			if !ok {
				player.SendMessage("&cWorld '" + portal.TargetWorld + "' is not loaded.")
				return
			}
			pp.mu.Lock()
			pp.cooldowns[player.Username] = time.Now()
			pp.mu.Unlock()
			player.ChangeWorld(target, false)
			return
		}
	}
}

type LogPlugin struct{ BasePlugin }

func (l *LogPlugin) Name() string { return "EventLogger" }
func (l *LogPlugin) OnPlayerJoin(p *Player, w *World) EventResult {
	cpeStatus := "Vanilla"
	if p.SupportsCPE {
		cpeStatus = "CPE"
		if p.SupportsBlockDefs {
			cpeStatus += "+BlockDefs"
		}
		if p.SupportsHeldBlock {
			cpeStatus += "+HeldBlock"
		}
		if p.SupportsTexturePack {
			cpeStatus += "+EnvMap"
		}
	}
	log.Printf("[Log] %s → joined %q [%s]", p.Username, w.Name, cpeStatus)
	return EventContinue
}
func (l *LogPlugin) OnPlayerLeave(p *Player, w *World) {
	log.Printf("[Log] %s ← left %q", p.Username, w.Name)
}

// ═══════════════════════════════════════════════════════════════════
// GENERATORS
// ═══════════════════════════════════════════════════════════════════

func generateHubWorld() *World {
	const (
		W, H, L = 64, 32, 64
	)
	world := &World{
		Width: W, Height: H, Length: L,
		Blocks: make([]byte, W*H*L),
		SpawnX: 31 * 32, SpawnY: 17 * 32, SpawnZ: 31 * 32,
	}

	set := func(x, y, z int, id byte) {
		if x >= 0 && x < W && y >= 0 && y < H && z >= 0 && z < L {
			world.Blocks[world.blockIndex(x, y, z)] = id
		}
	}

	for x := 0; x < W; x++ {
		for z := 0; z < L; z++ {
			for y := 0; y < 14; y++ {
				set(x, y, z, 1)
			}
			set(x, 14, z, 3)
			set(x, 15, z, 2)
		}
	}

	for x := 27; x <= 36; x++ {
		for z := 27; z <= 36; z++ {
			set(x, 15, z, 43)
			set(x, 21, z, 17)
			for y := 16; y <= 20; y++ {
				if x == 27 || x == 36 || z == 27 || z == 36 {
					set(x, y, z, 5)
				}
			}
		}
	}

	const bTeal = byte(26)
	const bGrey = byte(35)
	const bOrange = byte(22)

	buildDoor(world, set, 31, 16, 27, bTeal, "world1")
	buildDoor(world, set, 27, 16, 31, bGrey, "world2")
	buildDoor(world, set, 36, 16, 31, bOrange, "world3")

	set(31, 16, 36, 0)
	set(31, 17, 36, 0)
	set(31, 18, 36, 0)
	set(32, 16, 36, 0)
	set(32, 17, 36, 0)
	set(32, 18, 36, 0)

	return world
}

func buildDoor(w *World, set func(int, int, int, byte), x, y, z int, frame byte, target string) {
	if x == 27 || x == 36 {
		set(x, y+3, z, frame)
		set(x, y+3, z+1, frame)
		set(x, y, z-1, frame)
		set(x, y+1, z-1, frame)
		set(x, y+2, z-1, frame)
		set(x, y, z+2, frame)
		set(x, y+1, z+2, frame)
		set(x, y+2, z+2, frame)

		set(x, y, z, 0)
		set(x, y+1, z, 0)
		set(x, y+2, z, 0)
		set(x, y, z+1, 0)
		set(x, y+1, z+1, 0)
		set(x, y+2, z+1, 0)
		w.Portals = append(w.Portals, Portal{int16(x - 1), int16(y), int16(z), int16(x + 1), int16(y + 2), int16(z + 1), target})
	} else {
		set(x, y+3, z, frame)
		set(x+1, y+3, z, frame)
		set(x-1, y, z, frame)
		set(x-1, y+1, z, frame)
		set(x-1, y+2, z, frame)
		set(x+2, y, z, frame)
		set(x+2, y+1, z, frame)
		set(x+2, y+2, z, frame)

		set(x, y, z, 0)
		set(x, y+1, z, 0)
		set(x, y+2, z, 0)
		set(x+1, y, z, 0)
		set(x+1, y+1, z, 0)
		set(x+1, y+2, z, 0)
		w.Portals = append(w.Portals, Portal{int16(x), int16(y), int16(z - 1), int16(x + 1), int16(y + 2), int16(z + 1), target})
	}
}

func generateFlatWorld(w, h, l int16) *World {
	blocks := make([]byte, int(w)*int(h)*int(l))
	world := &World{
		Width: w, Height: h, Length: l, Blocks: blocks,
		SpawnX: (w / 2) * 32, SpawnY: (h/2 + 2) * 32, SpawnZ: (l / 2) * 32,
	}
	half := int(h / 2)
	for y := 0; y < int(h); y++ {
		for z := 0; z < int(l); z++ {
			for x := 0; x < int(w); x++ {
				idx := world.blockIndex(x, y, z)
				if y < half-1 {
					blocks[idx] = 1
				} else if y == half-1 {
					blocks[idx] = 2
				}
			}
		}
	}
	return world
}

// ═══════════════════════════════════════════════════════════════════
// PROTOCOL & MAP SENDING
// ═══════════════════════════════════════════════════════════════════

func sendMapToPlayer(conn net.Conn, world *World, supportsCPE bool) {
	writeUint8(conn, 0x02)

	world.mu.RLock()
	snapshot := make([]byte, len(world.Blocks))

	if supportsCPE {
		copy(snapshot, world.Blocks)
	} else {
		for i, b := range world.Blocks {
			if b > 49 {
				snapshot[i] = getFallbackBlock(b)
			} else {
				snapshot[i] = b
			}
		}
	}
	world.mu.RUnlock()

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	binary.Write(gz, binary.BigEndian, int32(len(snapshot)))
	gz.Write(snapshot)
	gz.Close()

	compressed := buf.Bytes()
	total := len(compressed)

	const chunkSize = 1024
	for i := 0; i < total; i += chunkSize {
		end := i + chunkSize
		if end > total {
			end = total
		}
		chunk := compressed[i:end]
		progress := byte(i * 100 / total)

		size := chunkSize
		pkt := make([]byte, 0, 1+2+chunkSize+1)
		pkt = append(pkt, 0x03, byte(size>>8), byte(size))
		padded := make([]byte, chunkSize)
		copy(padded, chunk)
		pkt = append(pkt, padded...)
		pkt = append(pkt, progress)
		conn.Write(pkt)
	}

	writeUint8(conn, 0x04)
	writeInt16(conn, world.Width)
	writeInt16(conn, world.Height)
	writeInt16(conn, world.Length)
}

func writePacket00(w io.Writer, version byte, name, motd string, magic byte) {
	w.Write([]byte{0x00, version})
	w.Write(padString(name))
	w.Write(padString(motd))
	w.Write([]byte{magic})
}

// disconnectPlayer sends a Disconnect (0x0E) packet with a reason message.
func disconnectPlayer(conn net.Conn, reason string) {
	pkt := make([]byte, 65)
	pkt[0] = 0x0E
	copy(pkt[1:65], padString(reason))
	conn.Write(pkt)
}

func padString(s string) []byte {
	buf := [64]byte{}
	for i := range buf {
		buf[i] = ' '
	}
	copy(buf[:], s)
	return buf[:]
}

func writeUint8(w io.Writer, b byte)    { w.Write([]byte{b}) }
func writeInt16(w io.Writer, i int16)   { binary.Write(w, binary.BigEndian, i) }
func writeInt32(w io.Writer, i int32)   { binary.Write(w, binary.BigEndian, i) }
func writeString(w io.Writer, s string) { w.Write(padString(s)) }

// ═══════════════════════════════════════════════════════════════════
// PIXEL ART GENERATOR — /pixelart <url> [maxsize]
// ═══════════════════════════════════════════════════════════════════

type blockColor struct {
	ID byte
	R  uint8
	G  uint8
	B  uint8
}

// blockPalette maps block IDs to their representative RGB color.
// Includes vanilla Classic blocks and the custom Crayola wool blocks.
var blockPalette = []blockColor{
	// ── Vanilla blocks ──
	{1, 125, 125, 125},  // Stone
	{3, 134, 96, 67},    // Dirt
	{4, 115, 115, 115},  // Cobblestone
	{5, 188, 152, 98},   // Planks
	{12, 218, 210, 158}, // Sand
	{17, 155, 125, 76},  // Log
	{20, 175, 213, 236}, // Glass (light blue tint)
	{21, 163, 45, 45},   // Red Wool
	{22, 217, 131, 58},  // Orange Wool
	{23, 177, 166, 39},  // Yellow Wool
	{24, 65, 174, 56},   // Lime Wool
	{25, 40, 117, 47},   // Green Wool
	{26, 40, 117, 107},  // Teal Wool
	{27, 58, 82, 165},   // Aqua Wool
	{28, 57, 64, 194},   // Cyan Wool (Blue)
	{29, 95, 54, 164},   // Indigo Wool
	{30, 126, 61, 181},  // Violet Wool
	{31, 172, 74, 181},  // Magenta Wool
	{32, 208, 132, 153}, // Pink Wool
	{33, 27, 27, 27},    // Black Wool
	{34, 157, 157, 157}, // Gray Wool
	{35, 255, 255, 255}, // White Wool
	{36, 223, 223, 223}, // Light Gray Wool
	{41, 231, 165, 45},  // Gold Block
	{42, 191, 191, 191}, // Iron Block (silver)
	{43, 175, 175, 175}, // Double Slab
	{44, 175, 175, 175}, // Slab
	{45, 155, 93, 83},   // Brick
	// ── Custom Crayola wool blocks ──
	{102, 239, 222, 205}, // Almond
	{103, 253, 217, 181}, // Apricot
	{104, 253, 114, 114}, // Bittersweet
	{105, 222, 93, 131},  // Blush
	{106, 203, 65, 84},   // Brick Red
	{107, 255, 170, 204}, // Carnation Pink
	{108, 221, 68, 146},  // Cerise
	{109, 188, 93, 88},   // Chestnut
	{110, 202, 55, 103},  // Jazzberry Jam
	{111, 205, 74, 76},   // Mahogany
	{112, 255, 130, 67},  // Mango Tango
	{113, 239, 152, 170}, // Mauvelous
	{114, 253, 188, 180}, // Melon
	{115, 255, 207, 171}, // Peach
	{116, 252, 116, 253}, // Pink Flamingo
	{117, 247, 143, 167}, // Pink Sherbert
	{118, 195, 100, 197}, // Fuchsia
	{186, 255, 53, 94},   // Radical Red
	{119, 227, 37, 107},  // Razzmatazz
	{120, 238, 32, 77},   // Red
	{121, 255, 155, 170}, // Salmon
	{122, 252, 40, 71},   // Scarlet
	{123, 253, 94, 83},   // Sunset Orange
	{124, 252, 137, 172}, // Tickle Me Pink
	{125, 247, 83, 148},  // Violet Red
	{126, 255, 67, 164},  // Wild Strawberry
	{127, 255, 164, 116}, // Atomic Tangerine
	{128, 255, 127, 73},  // Burnt Orange
	{129, 234, 126, 93},  // Burnt Sienna
	{130, 184, 115, 51},  // Copper
	{131, 255, 189, 136}, // Macaroni And Cheese
	{132, 255, 163, 67},  // Neon Carrot
	{133, 255, 117, 56},  // Orange
	{134, 255, 110, 74},  // Outrageous Orange
	{135, 255, 83, 73},   // Red Orange
	{136, 222, 170, 136}, // Tumbleweed
	{137, 255, 160, 137}, // Vivid Tangerine
	{138, 250, 231, 181}, // Banana Mania
	{139, 255, 255, 153}, // Canary
	{140, 253, 219, 109}, // Dandelion
	{141, 239, 205, 184}, // Desert Sand
	{142, 231, 198, 151}, // Gold
	{143, 252, 214, 103}, // Goldenrod
	{144, 253, 252, 116}, // Laser Lemon
	{145, 255, 207, 72},  // Sunglow
	{146, 250, 167, 108}, // Tan
	{147, 253, 252, 116}, // Unmellow Yellow
	{148, 252, 232, 131}, // Yellow
	{149, 255, 182, 83},  // Yellow Orange
	{150, 206, 255, 29},  // Electric Lime
	{151, 240, 232, 145}, // Green Yellow
	{152, 178, 236, 93},  // Inchworm
	{153, 186, 184, 108}, // Olive Green
	{154, 236, 235, 189}, // Spring Green
	{155, 197, 227, 132}, // Yellow Green
	{156, 135, 169, 107}, // Asparagus
	{157, 113, 188, 120}, // Fern
	{158, 109, 174, 129}, // Forest Green
	{159, 168, 228, 160}, // Granny Smith Apple
	{160, 28, 172, 120},  // Green
	{161, 59, 176, 143},  // Jungle Green
	{162, 30, 168, 132},  // Mountain Meadow
	{163, 21, 128, 120},  // Pine Green
	{164, 118, 255, 122}, // Screamin Green
	{165, 147, 223, 184}, // Sea Green
	{166, 69, 206, 162},  // Shamrock
	{167, 23, 128, 109},  // Tropical Rain Forest
	{168, 120, 219, 226}, // Aquamarine
	{169, 31, 117, 254},  // Blue
	{170, 13, 152, 186},  // Blue Green
	{171, 28, 211, 162},  // Caribbean Green
	{172, 29, 172, 214},  // Cerulean
	{173, 154, 206, 235}, // Cornflower
	{174, 43, 108, 196},  // Denim
	{175, 0, 51, 100},    // Midnight Blue
	{176, 25, 116, 210},  // Navy Blue
	{177, 28, 169, 201},  // Pacific Blue
	{178, 31, 206, 203},  // Robin's Egg Blue
	{179, 128, 218, 235}, // Sky Blue
	{180, 219, 215, 210}, // Timberwolf
	{181, 119, 221, 231}, // Turquoise Blue
	{182, 162, 162, 208}, // Blue Bell
	{183, 115, 102, 189}, // Blue Violet
	{184, 176, 183, 198}, // Cadet Blue
	{185, 110, 81, 96},   // Eggplant
	// 186 = Radical Red (already above)
	{187, 93, 118, 203},  // Indigo
	{188, 252, 180, 213}, // Lavender
	{189, 151, 154, 170}, // Manatee
	{190, 197, 75, 140},  // Mulberry
	{191, 230, 168, 215}, // Orchid
	{192, 65, 74, 76},    // Outer Space
	{193, 197, 208, 230}, // Periwinkle
	{194, 142, 69, 133},  // Plum
	{195, 116, 66, 200},  // Purple Heart
	{196, 157, 129, 186}, // Purple Mountain Majesty
	{197, 254, 78, 218},  // Purple Pizzazz
	{198, 255, 72, 208},  // Razzle Dazzle Rose
	{199, 192, 68, 143},  // Red Violet
	{200, 120, 81, 169},  // Royal Purple
	{201, 251, 126, 253}, // Shocking Pink
	{202, 235, 199, 223}, // Thistle
	{203, 143, 80, 157},  // Vivid Violet
	{204, 146, 110, 174}, // Violet (Purple)
	{205, 162, 173, 208}, // Wild Blue Yonder
	{206, 205, 164, 222}, // Wisteria
	{207, 255, 29, 206},  // Hot Magenta
	{208, 246, 100, 175}, // Magenta
	{209, 252, 108, 133}, // Wild Watermelon
	{210, 205, 149, 117}, // Antique Brass
	{211, 172, 229, 238}, // Blizzard Blue
	{212, 200, 56, 90},   // Maroon
	{213, 165, 105, 79},  // Sepia
	{214, 138, 121, 93},  // Shadow
	{215, 159, 129, 112}, // Beaver
	{216, 35, 35, 35},    // Black
	{217, 180, 103, 77},  // Brown
	{218, 204, 102, 102}, // Fuzzy Wuzzy
	{219, 149, 145, 140}, // Gray
	{220, 205, 197, 194}, // Silver
	{221, 237, 237, 237}, // White
}

func nearestBlock(r, g, b uint8) byte {
	best := byte(35) // default white
	bestDist := math.MaxFloat64

	for _, bc := range blockPalette {
		dr := float64(int(r) - int(bc.R))
		dg := float64(int(g) - int(bc.G))
		db := float64(int(b) - int(bc.B))
		// Weighted Euclidean — human eyes are more sensitive to green
		dist := 2*dr*dr + 4*dg*dg + 3*db*db
		if dist < bestDist {
			bestDist = dist
			best = bc.ID
		}
	}
	return best
}

// scaleImage does nearest-neighbor resize to fit within maxW × maxH.
func scaleImage(src image.Image, maxW, maxH int) image.Image {
	bounds := src.Bounds()
	srcW := bounds.Dx()
	srcH := bounds.Dy()

	if srcW <= maxW && srcH <= maxH {
		return src
	}

	scale := math.Min(float64(maxW)/float64(srcW), float64(maxH)/float64(srcH))
	dstW := int(math.Max(1, math.Round(float64(srcW)*scale)))
	dstH := int(math.Max(1, math.Round(float64(srcH)*scale)))

	dst := image.NewRGBA(image.Rect(0, 0, dstW, dstH))
	for y := 0; y < dstH; y++ {
		srcY := int(float64(y) / scale)
		if srcY >= srcH {
			srcY = srcH - 1
		}
		for x := 0; x < dstW; x++ {
			srcX := int(float64(x) / scale)
			if srcX >= srcW {
				srcX = srcW - 1
			}
			dst.Set(x, y, src.At(bounds.Min.X+srcX, bounds.Min.Y+srcY))
		}
	}
	return dst
}

// placePixelArt downloads an image from url, scales it, and places it
// in the world as a vertical wall facing the player's direction.
func placePixelArt(player *Player, server *Server, imageURL string, maxSize int) {
	player.SendMessage("&eDownloading image...")

	client := &http.Client{Timeout: 15 * time.Second}
	req, err := http.NewRequest("GET", imageURL, nil)
	if err != nil {
		player.SendMessage("&cBad URL: " + err.Error())
		return
	}
	req.Header.Set("User-Agent", "GoClassic/1.0")
	resp, err := client.Do(req)
	if err != nil {
		player.SendMessage("&cDownload failed: " + err.Error())
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		player.SendMessage(fmt.Sprintf("&cHTTP %d from server.", resp.StatusCode))
		return
	}

	// Limit download to 10MB
	limitedBody := io.LimitReader(resp.Body, 10*1024*1024)
	img, _, err := image.Decode(limitedBody)
	if err != nil {
		player.SendMessage("&cFailed to decode image: " + err.Error())
		return
	}

	scaled := scaleImage(img, maxSize, maxSize)
	bounds := scaled.Bounds()
	imgW := bounds.Dx()
	imgH := bounds.Dy()

	player.SendMessage(fmt.Sprintf("&ePlacing %dx%d pixel art...", imgW, imgH))

	if player.World == nil {
		player.SendMessage("&cYou must be in a world.")
		return
	}

	// Check world bounds
	w := player.World
	if imgW > int(w.Width) || imgH > int(w.Height) {
		player.SendMessage("&cImage is too large for this world.")
		return
	}

	// Determine facing direction from yaw to decide which axis to paint on.
	// Yaw: 0=south(+Z), 64=west(-X), 128=north(-Z), 192=east(+X)
	bx := int(player.X / 32)
	by := int(player.Y/32) - 1 // feet level
	bz := int(player.Z / 32)

	yaw := player.Yaw
	type direction struct{ dx, dz int }
	var faceDir direction // direction we step across for columns
	var placeOnZ bool     // true = wall runs along X axis, false = along Z axis

	switch {
	case yaw < 32 || yaw >= 224: // facing south (+Z)
		faceDir = direction{1, 0}
		bz += 2
		placeOnZ = false
	case yaw >= 32 && yaw < 96: // facing west (-X)
		faceDir = direction{0, 1}
		bx -= 2
		placeOnZ = true
	case yaw >= 96 && yaw < 160: // facing north (-Z)
		faceDir = direction{-1, 0}
		bz -= 2
		placeOnZ = false
	default: // facing east (+X)
		faceDir = direction{0, -1}
		bx += 2
		placeOnZ = true
	}
	_ = placeOnZ

	// Center the image horizontally
	startX := bx - (imgW/2)*faceDir.dx
	startZ := bz - (imgW/2)*faceDir.dz

	placed := 0
	for px := 0; px < imgW; px++ {
		for py := 0; py < imgH; py++ {
			r, g, b, a := scaled.At(bounds.Min.X+px, bounds.Min.Y+py).RGBA()
			// Skip fully transparent pixels
			if a < 0x8000 {
				continue
			}
			blockID := nearestBlock(uint8(r>>8), uint8(g>>8), uint8(b>>8))

			wx := startX + px*faceDir.dx
			wy := by + (imgH - 1 - py) // bottom-up so image isn't flipped
			wz := startZ + px*faceDir.dz

			if wx < 0 || wx >= int(w.Width) || wy < 0 || wy >= int(w.Height) || wz < 0 || wz >= int(w.Length) {
				continue
			}

			oldBlock := w.GetBlock(wx, wy, wz)
			w.SetBlock(wx, wy, wz, blockID)
			recordBlockChange(w.Name, "[pixelart:"+player.Username+"]", int16(wx), int16(wy), int16(wz), oldBlock, blockID)

			// Broadcast to all players in the world
			server.mu.RLock()
			for _, p2 := range server.players {
				if p2.World == w {
					sendBlockUpdate(p2.Conn, int16(wx), int16(wy), int16(wz), blockID, p2.SupportsCPE)
				}
			}
			server.mu.RUnlock()
			placed++
		}
	}

	player.SendMessage(fmt.Sprintf("&aPixel art placed! %d blocks (%dx%d)", placed, imgW, imgH))
	log.Printf("[PixelArt] %s placed %dx%d (%d blocks) in %q from %s",
		player.Username, imgW, imgH, placed, w.Name, imageURL)
}

// ═══════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════

func main() {
	loadConfig()
	loadConfig()
	loadCustomBlocks()
	loadBanList()
	initDatabase()

	// Generate a unique salt for this session (used by heartbeat + name verification)
	serverSalt = generateSalt()

	// Auto-import MCGalaxy global.json if present (merges with customblocks.json)
	if _, err := os.Stat("global.json"); err == nil {
		imported, skipped, err := importMCGalaxyBlocks("global.json")
		if err != nil {
			log.Printf("[Blocks] Failed to import global.json: %v", err)
		} else if imported > 0 {
			log.Printf("[Blocks] Auto-imported %d block(s) from global.json (%d skipped)", imported, skipped)
			saveCustomBlocks() // persist the merged result
		}
	}

	server := NewServer()

	server.RegisterPlugin(&HubProtectionPlugin{})
	server.RegisterPlugin(NewPortalPlugin())
	server.RegisterPlugin(&LogPlugin{})

	hub, err := LoadMCGalaxyLevel("levels/hub.lvl")
	if err != nil {
		hub = generateHubWorld()
		SaveMCGalaxyLevel(hub, "levels/hub.lvl")
		savePortals(hub.Portals, "levels/hub.portals.json")
	} else {
		hub.Portals = loadPortals("levels/hub.portals.json")
	}
	server.AddWorld("hub", hub)

	configs := []string{"world1", "world2", "world3"}
	for _, name := range configs {
		path := "levels/" + name + ".lvl"
		world, err := LoadMCGalaxyLevel(path)
		if err != nil {
			log.Printf("[World]  %s not found — using generated flat world", path)
			world = generateFlatWorld(512, 32, 512)
			SaveMCGalaxyLevel(world, path)
		}
		server.AddWorld(name, world)
	}

	go func() {
		saveCount := 0
		for {
			time.Sleep(30 * time.Second)
			server.mu.RLock()
			for name, w := range server.worlds {
				SaveMCGalaxyLevel(w, "levels/"+name+".lvl")
				savePortals(w.Portals, "levels/"+name+".portals.json")
			}
			server.mu.RUnlock()

			// Prune history older than 30 days every ~15 minutes
			saveCount++
			if saveCount%30 == 0 {
				pruneBlockHistory(30 * 24 * time.Hour)
			}
		}
	}()

	customBlocksMu.RLock()
	cbCount := len(customBlocks)
	customBlocksMu.RUnlock()
	log.Printf("[Blocks] %d custom block definition(s) loaded", cbCount)

	banMutex.RLock()
	banCount := len(bannedPlayers)
	banMutex.RUnlock()
	if banCount > 0 {
		log.Printf("[Bans] %d banned player(s) loaded", banCount)
	}

	// Start built-in HTTP server for terrain.png
	startHTTPServer()

	// Start ClassiCube server list heartbeat
	if serverConfig.TerrainURL != "" {
		log.Printf("[Texture] Clients will download: %s", serverConfig.TerrainURL)
		if strings.HasSuffix(serverConfig.TerrainURL, ".png") {
			log.Printf("[Texture] WARNING: ClassiCube expects a .zip URL, not .png")
			log.Printf("[Texture] Change TerrainURL to end with /terrain.zip")
		}
	}
	startHeartbeat(server)

	ln, err := net.Listen("tcp", serverConfig.Port)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", serverConfig.Port, err)
	}

	log.Printf("%s listening on %s (max %d players)", serverConfig.ServerName, serverConfig.Port, serverConfig.MaxPlayers)
	if serverConfig.Public {
		log.Printf("[Heartbeat] Public=true, VerifyNames=%v — listing on classicube.net", serverConfig.VerifyNames)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go handleConnection(conn, server)
	}
}
