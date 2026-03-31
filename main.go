package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
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
}

var serverConfig Config

func hashPassword(password string) string {
	hash := sha256.Sum256([]byte(password))
	return hex.EncodeToString(hash[:])
}

func saveConfig() {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("ServerName=%s\n", serverConfig.ServerName))
	buf.WriteString(fmt.Sprintf("Motd=%s\n", serverConfig.Motd))
	buf.WriteString(fmt.Sprintf("Port=%s\n", strings.TrimPrefix(serverConfig.Port, ":")))

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
	}

	f, err := os.Open("server.properties")
	if err != nil {
		defaultWorldHash := hashPassword("letmein")
		defaultOpHash := hashPassword("admin")

		defaultProps := fmt.Sprintf("ServerName=Go Classic Server\nMotd=Welcome to the server!\nPort=25565\npass_world2=%s\nOpPassword=%s\nBannedBlocks=8,9,10,11\n", defaultWorldHash, defaultOpHash)
		os.WriteFile("server.properties", []byte(defaultProps), 0644)
		serverConfig.WorldPasswords["world2"] = defaultWorldHash
		serverConfig.OpPassword = defaultOpHash
		serverConfig.BannedBlocks = []byte{8, 9, 10, 11, 54}
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

type PlayerState struct {
	World          string            `json:"world"`
	X              int16             `json:"x"`
	Y              int16             `json:"y"`
	Z              int16             `json:"z"`
	Yaw            byte              `json:"yaw"`
	Pitch          byte              `json:"pitch"`
	SavedPasswords map[string]string `json:"saved_passwords"`
}

var (
	playerDB = make(map[string]PlayerState)
	dbMutex  sync.Mutex
)

func loadPlayerDB() {
	data, err := os.ReadFile("players.json")
	if err == nil {
		json.Unmarshal(data, &playerDB)
	}
}

func savePlayerDB() {
	dbMutex.Lock()
	data, _ := json.MarshalIndent(playerDB, "", "  ")
	dbMutex.Unlock()
	os.WriteFile("players.json", data, 0644)
}

func updatePlayerState(p *Player) {
	if p.World == nil {
		return
	}

	p.mu.Lock()
	savedPws := make(map[string]string)
	for k, v := range p.SavedPasswords {
		savedPws[k] = v
	}
	p.mu.Unlock()

	dbMutex.Lock()
	playerDB[p.Username] = PlayerState{
		World: p.World.Name,
		X:     p.X, Y: p.Y, Z: p.Z,
		Yaw: p.Yaw, Pitch: p.Pitch,
		SavedPasswords: savedPws,
	}
	dbMutex.Unlock()
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
	mu      sync.RWMutex
}

func NewServer() *Server {
	return &Server{
		worlds:  make(map[string]*World),
		players: make(map[string]*Player),
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
	Conn              net.Conn
	Username          string
	World             *World
	Server            *Server
	X, Y, Z           int16
	Yaw, Pitch        byte
	Authenticated     map[string]bool
	SavedPasswords    map[string]string
	PendingWorld      *World
	SupportsCPE       bool // client negotiated CustomBlocks (ids 50-65 natively)
	SupportsBlockDefs bool // client negotiated BlockDefinitions CPE extension
	SupportsHeldBlock bool // client negotiated HeldBlock CPE extension
	IsOp              bool
	mu                sync.Mutex
}

func (p *Player) SendMessage(msg string) {
	pkt := make([]byte, 66)
	pkt[0] = 0x0D
	pkt[1] = 0xFF
	copy(pkt[2:], padString(msg))
	p.Conn.Write(pkt)
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

	writeUint8(p.Conn, 0x08)
	writeUint8(p.Conn, 255)
	writeInt16(p.Conn, sx)
	writeInt16(p.Conn, sy)
	writeInt16(p.Conn, sz)
	writeUint8(p.Conn, syaw)
	writeUint8(p.Conn, spitch)

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

	cpe := buf[130] == 0x42
	magic := byte(0x00)
	if cpe {
		magic = 0x42
	}

	// Server identification MUST be sent before the CPE handshake
	writePacket00(conn, 7, serverConfig.ServerName, serverConfig.Motd, magic)

	// ── CPE Handshake ──────────────────────────────────────────
	clientSupportsCustomBlocks := false
	clientSupportsBlockDefs := false
	clientSupportsHeldBlock := false

	if cpe {
		// Advertise three extensions
		writeUint8(conn, 0x10) // ExtInfo
		writeString(conn, serverConfig.ServerName)
		writeInt16(conn, 3) // 3 extensions

		writeUint8(conn, 0x11) // ExtEntry 1
		writeString(conn, "CustomBlocks")
		writeInt32(conn, 1)

		writeUint8(conn, 0x11) // ExtEntry 2
		writeString(conn, "BlockDefinitions")
		writeInt32(conn, 1)

		writeUint8(conn, 0x11) // ExtEntry 3
		writeString(conn, "HeldBlock")
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
		Conn:              conn,
		Username:          username,
		Server:            server,
		Authenticated:     make(map[string]bool),
		SavedPasswords:    make(map[string]string),
		SupportsCPE:       clientSupportsCustomBlocks,
		SupportsBlockDefs: clientSupportsBlockDefs,
		SupportsHeldBlock: clientSupportsHeldBlock,
	}

	// Push all custom block definitions BEFORE the first map is sent
	if clientSupportsBlockDefs {
		sendAllCustomBlocks(conn)
	}

	server.mu.Lock()
	server.players[username] = player
	server.mu.Unlock()

	defer func() {
		server.mu.Lock()
		delete(server.players, username)
		server.mu.Unlock()
		if player.World != nil {
			for _, pl := range server.plugins {
				pl.OnPlayerLeave(player, player.World)
			}
		}
	}()

	dbMutex.Lock()
	savedState, hasSavedState := playerDB[username]
	dbMutex.Unlock()

	worldSet := false
	if hasSavedState {
		if savedState.SavedPasswords != nil {
			for k, v := range savedState.SavedPasswords {
				player.SavedPasswords[k] = v
			}
		}
		if w, ok := server.GetWorld(savedState.World); ok {
			player.X, player.Y, player.Z = savedState.X, savedState.Y, savedState.Z
			player.Yaw, player.Pitch = savedState.Yaw, savedState.Pitch
			player.ChangeWorld(w, true)
			worldSet = true
		}
	}

	if !worldSet {
		if hub, ok := server.GetWorld("hub"); ok {
			player.ChangeWorld(hub, false)
		}
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
				lockStr := ""
				if lock == 1 {
					lockStr = " &7(locked)"
				}
				player.SendMessage(fmt.Sprintf("&aHeld block set to %d.%s", id, lockStr))
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
// MAIN
// ═══════════════════════════════════════════════════════════════════

func main() {
	loadConfig()
	loadPlayerDB()
	loadCustomBlocks()

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
		for {
			time.Sleep(30 * time.Second)
			savePlayerDB()
			server.mu.RLock()
			for name, w := range server.worlds {
				SaveMCGalaxyLevel(w, "levels/"+name+".lvl")
				savePortals(w.Portals, "levels/"+name+".portals.json")
			}
			server.mu.RUnlock()
		}
	}()

	customBlocksMu.RLock()
	cbCount := len(customBlocks)
	customBlocksMu.RUnlock()
	log.Printf("[Blocks] %d custom block definition(s) loaded", cbCount)

	ln, err := net.Listen("tcp", serverConfig.Port)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", serverConfig.Port, err)
	}

	log.Printf("%s listening on %s", serverConfig.ServerName, serverConfig.Port)

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go handleConnection(conn, server)
	}
}
