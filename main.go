package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
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
		defaultProps := "ServerName=Go Classic Server\nMotd=Welcome to the server!\nPort=25565\npass_world2=letmein\nOpPassword=admin\nBannedBlocks=8,9,10,11\n"
		os.WriteFile("server.properties", []byte(defaultProps), 0644)
		serverConfig.WorldPasswords["world2"] = "letmein"
		serverConfig.OpPassword = "admin"
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
	SupportsCPE       bool
	SupportsHeldBlock bool
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

// Fallback mechanism for Vanilla Classic Clients
func getFallbackBlock(b byte) byte {
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

	// 0x08 is used for teleporting/spawning the local player correctly.
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
// CPE BLOCK DEFINITIONS
// ═══════════════════════════════════════════════════════════════════

type CPEBlock struct {
	ID             byte
	Name           string
	Tex            byte
	CollideType    byte
	TransmitsLight byte
	Sound          byte
	FullBright     byte
	Shape          byte
	Draw           byte
}

var extraBlocks = []CPEBlock{
	{50, "Cobblestone Slab", 16, 2, 0, 4, 0, 8, 0},
	{51, "Rope", 11, 0, 0, 7, 0, 16, 1},
	{52, "Sandstone", 22, 2, 0, 4, 0, 16, 0},
	{53, "Snow", 23, 2, 0, 9, 0, 16, 0},
	{54, "Fire", 24, 0, 1, 0, 1, 16, 1},
	{55, "Light Pink", 25, 2, 0, 7, 0, 16, 0},
	{56, "Forest Green", 26, 2, 0, 7, 0, 16, 0},
	{57, "Brown", 27, 2, 0, 7, 0, 16, 0},
	{58, "Deep Blue", 28, 2, 0, 7, 0, 16, 0},
	{59, "Turquoise", 29, 2, 0, 7, 0, 16, 0},
	{60, "Ice", 30, 2, 0, 6, 0, 16, 3},
	{61, "Ceramic Tile", 31, 2, 0, 4, 0, 16, 0},
	{62, "Magma", 32, 2, 0, 4, 1, 16, 0},
	{63, "Pillar", 33, 2, 0, 4, 0, 16, 0},
	{64, "Crate", 34, 2, 0, 1, 0, 16, 0},
	{65, "Stone Brick", 35, 2, 0, 4, 0, 16, 0},
}

func sendBlockDefinitions(conn net.Conn) {
	for _, b := range extraBlocks {
		pkt := make([]byte, 83)
		pkt[0] = 0x22
		pkt[1] = b.ID
		copy(pkt[2:66], padString(b.Name))

		pkt[66] = b.CollideType
		binary.BigEndian.PutUint32(pkt[67:71], 0x3F800000) // Speed float32 = 1.0

		pkt[71] = b.Tex // Top
		pkt[72] = b.Tex // Bottom
		pkt[73] = b.Tex // Side

		pkt[74] = b.TransmitsLight
		pkt[75] = b.Sound
		pkt[76] = b.FullBright
		pkt[77] = b.Shape
		pkt[78] = b.Draw
		// 79-82 left as 0 for Fog data

		conn.Write(pkt)
	}
}

// ═══════════════════════════════════════════════════════════════════
// CONNECTION HANDLER & COMMANDS
// ═══════════════════════════════════════════════════════════════════

var clientPacketSizes = map[byte]int{
	0x05: 8,
	0x08: 9,
	0x0D: 65,
	0x10: 66,
	0x11: 68,
	0x13: 1, // <--- CHANGED FROM 0x12 to 0x13
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

	// ALWAYS send ServerIdentification first
	//writePacket00(conn, 7, serverConfig.ServerName, serverConfig.Motd, magic)

	// --- CPE Handshake ---
	clientSupportsCustomBlocks := false
	clientSupportsBlockDefs := false
	clientSupportsHeldBlock := false

	if cpe {
		writeUint8(conn, 0x10)
		writeString(conn, serverConfig.ServerName)
		writeInt16(conn, 3) // Announcing 3 extensions

		writeUint8(conn, 0x11)
		writeString(conn, "CustomBlocks")
		writeInt32(conn, 1)

		writeUint8(conn, 0x11)
		writeString(conn, "BlockDefinitions")
		writeInt32(conn, 1)

		writeUint8(conn, 0x11)
		writeString(conn, "HeldBlock")
		writeInt32(conn, 1)

		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		expectedEntries := -1
		entriesRead := 0

		for expectedEntries == -1 || entriesRead < expectedEntries {
			pidBuf := make([]byte, 1)
			if _, err := io.ReadFull(conn, pidBuf); err != nil {
				break
			}

			if pidBuf[0] == 0x10 {
				extInfo := make([]byte, 66)
				io.ReadFull(conn, extInfo)
				expectedEntries = int(binary.BigEndian.Uint16(extInfo[64:66]))
				if expectedEntries == 0 {
					break
				}
			} else if pidBuf[0] == 0x11 {
				extEntry := make([]byte, 68)
				io.ReadFull(conn, extEntry)
				extName := strings.TrimRight(string(extEntry[0:64]), " ")

				if extName == "CustomBlocks" {
					clientSupportsCustomBlocks = true
				} else if extName == "BlockDefinitions" {
					clientSupportsBlockDefs = true
				} else if extName == "HeldBlock" {
					clientSupportsHeldBlock = true
				}
				entriesRead++
			} else {
				break // Unrecognized packet, safely end handshake
			}
		}

		if clientSupportsCustomBlocks {
			writeUint8(conn, 0x13) // <--- CHANGED FROM 0x12
			writeUint8(conn, 1)

			pidBuf := make([]byte, 1)
			if _, err := io.ReadFull(conn, pidBuf); err == nil && pidBuf[0] == 0x12 {
				io.ReadFull(conn, make([]byte, 1))
			}
		}
		conn.SetReadDeadline(time.Time{})
	}

	// 1. ✅ ALWAYS send ServerIdentification AFTER the CPE Handshake completes
	writePacket00(conn, 7, serverConfig.ServerName, serverConfig.Motd, magic)

	// 2. ✅ Send Block Definitions SAFELY AFTER ServerIdentification
	if clientSupportsBlockDefs {
		sendBlockDefinitions(conn)
	}

	player := &Player{
		Conn:              conn,
		Username:          username,
		Server:            server,
		Authenticated:     make(map[string]bool),
		SavedPasswords:    make(map[string]string),
		SupportsCPE:       clientSupportsCustomBlocks,
		SupportsHeldBlock: clientSupportsHeldBlock,
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

			if msg == "/hub" {
				if w, ok := server.GetWorld("hub"); ok {
					player.ChangeWorld(w, false)
				} else {
					player.SendMessage("&cHub world not found.")
				}
				continue
			}

			if strings.HasPrefix(msg, "/hand ") {
				idStr := strings.TrimSpace(strings.TrimPrefix(msg, "/hand"))
				id, err := strconv.Atoi(idStr)
				if err == nil && id >= 0 && id <= 255 {
					if player.SupportsHeldBlock {
						pkt := []byte{0x14, byte(id), 0} // 0 = Allows user to switch away
						player.Conn.Write(pkt)
						player.SendMessage(fmt.Sprintf("&aNow holding block %d", id))
					} else {
						player.SendMessage("&cYour client doesn't support the HeldBlock extension.")
					}
				} else {
					player.SendMessage("&cUsage: /hand <id> (0-255)")
				}
				continue
			}

			if strings.HasPrefix(msg, "/op ") {
				password := strings.TrimPrefix(msg, "/op ")
				if serverConfig.OpPassword != "" && password == serverConfig.OpPassword {
					player.IsOp = true
					player.SendMessage("&aYou are now a server operator.")
				} else {
					player.SendMessage("&cIncorrect operator password.")
				}
				continue
			}

			if strings.HasPrefix(msg, "/pass ") {
				password := strings.TrimPrefix(msg, "/pass ")
				player.mu.Lock()
				pending := player.PendingWorld
				player.mu.Unlock()

				if pending != nil && serverConfig.WorldPasswords[pending.Name] == password {
					player.mu.Lock()
					player.Authenticated[pending.Name] = true
					player.SavedPasswords[pending.Name] = password
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

			if strings.HasPrefix(msg, "/goto ") {
				name := strings.TrimPrefix(msg, "/goto ")
				if w, ok := server.GetWorld(name); ok {
					player.ChangeWorld(w, false)
				} else {
					player.SendMessage("&cWorld not found: &e" + name)
				}
				continue
			}

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
		cpeStatus = "CPE-Capable"
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
