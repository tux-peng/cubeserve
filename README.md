# GoClassic

A ClassiCube server written in Go with CPE support, multi-world portals, custom blocks, texture packs, block history, and a plugin system.

## Features

- **Multi-world** — hub world with portals to other worlds, password-protected worlds, `/goto` navigation
- **CPE extensions** — CustomBlocks, BlockDefinitions, HeldBlock, EnvMapAspect (texture packs)
- **Custom blocks** — define blocks 50–255 with full control over textures, solidity, shape, sound, and more
- **MCGalaxy compatibility** — import/export `global.json` block definitions, load `.lvl` level files
- **Custom texture packs** — built-in HTTP server serves `terrain.png` as a zip; clients download and apply it automatically
- **Public server list** — heartbeat to classicube.net with name verification
- **Player visibility** — players see each other, with real-time movement and cross-world spawn/despawn
- **Block history** — every block change is logged per-world with player, time, and old/new block IDs
- **Inspect & rollback** — ops can click blocks to see who changed them and revert a player's changes
- **Moderation** — kick, ban/unban with reasons, ban list persistence, operator system
- **Plugin system** — event-driven hooks for join, leave, chat, block change, and movement
- **Persistence** — worlds, player positions, passwords, bans, block history, and custom blocks all saved to disk

## Quick Start

```bash
go build -o goclassic main.go
./goclassic
```

On first run the server creates `server.properties` with defaults and generates a hub world with portals to three flat worlds. Connect with any ClassiCube client to `localhost:25565`.

## Configuration

All settings live in `server.properties`:

```properties
ServerName=Go Classic Server
Motd=Welcome to the server!
Port=25565
Public=false
MaxPlayers=128
VerifyNames=true
Software=GoClassic 0.9beta

# Texture pack — clients download this automatically
# TerrainURL=http://your-ip:8080/terrain.zip
# HTTPPort=8080

# Operator password (stored as SHA-256 hash)
OpPassword=...

# World passwords (stored as SHA-256 hashes)
pass_world2=...

# Block IDs players cannot place (comma-separated)
BannedBlocks=8,9,10,11
```

### Going Public

1. Set `Public=true` in `server.properties`
2. Port-forward `25565` (game) through your router
3. Restart — the server sends heartbeats to classicube.net every 45 seconds
4. Your server appears in the ClassiCube server list within a minute

When `VerifyNames=true` (default), players must join through classicube.net so their identity is verified via the heartbeat salt.

### Custom Textures

1. Place your `terrain.png` in the server directory
2. Set `HTTPPort=8080` and `TerrainURL=http://your-public-ip:8080/terrain.zip`
3. Port-forward `8080` (HTTP)
4. Restart — the server wraps `terrain.png` into a zip and serves it at `/terrain.zip`

The URL is sent to clients via the CPE EnvMapAspect extension after each map load. You can also host the zip elsewhere:

```properties
TerrainURL=http://example.com/my-textures.zip
```

Hit `http://localhost:8080/terrain/reload` to rebuild the zip after replacing `terrain.png` without restarting.

## Commands

### All Players

| Command | Description |
|---|---|
| `/goto <world>` | Teleport to another world |
| `/pass <password>` | Enter the password for a password-protected world |

### Operators

Authenticate first with `/op <password>` (default password: `admin`).

#### World Management

| Command | Description |
|---|---|
| `/newlvl <name> <w> <h> <l>` | Create a new flat world with the given dimensions |
| `/resizelvl <w> <h> <l>` | Resize the current world (preserves existing blocks) |

#### Custom Blocks (`/bg`)

| Command | Description |
|---|---|
| `/bg` | Show all subcommands |
| `/bg define <id> <name>` | Create a new block (ID 50–255) with stone defaults |
| `/bg set <id> <prop> <val>` | Edit a property (see `/bg props` for the full list) |
| `/bg remove <id>` | Delete a block definition |
| `/bg list` | Show all custom blocks |
| `/bg info <id>` | Show full details for a block |
| `/bg copy <src> <dst>` | Duplicate a block to a new ID |
| `/bg props` | List all editable properties with valid ranges |
| `/bg import [path]` | Import blocks from an MCGalaxy `global.json` (default: `global.json`) |
| `/bg export [path]` | Export blocks to MCGalaxy format (default: `global_export.json`) |

**Editable properties:**

| Property | Values | Description |
|---|---|---|
| `name` | text | Display name |
| `solidity` | 0–2 | 0=walk-through, 1=swim-through, 2=solid |
| `speed` | 1–8 | Movement speed multiplier (4=normal) |
| `toptex` | 0–255 | Texture atlas index for top face |
| `sidetex` | 0–255 | Texture atlas index for side faces |
| `bottomtex` | 0–255 | Texture atlas index for bottom face |
| `alltex` | 0–255 | Set all three textures at once |
| `light` | 0–1 | 0=blocks light, 1=transmits light |
| `sound` | 0–9 | Walk sound (0=none, 1=wood, 2=gravel, 3=grass, 4=stone, 5=metal, 6=glass, 7=wool, 8=sand, 9=snow) |
| `bright` | 0–1 | 0=normal, 1=full bright (glows) |
| `shape` | 0–16 | 0=sprite, 1–16=height in sixteenths |
| `draw` | 0–4 | 0=opaque, 1=transparent(same), 2=transparent(diff), 3=translucent, 4=gas |
| `fog` | d r g b | Fog density and RGB color (each 0–255) |
| `fallback` | 0–49 | Block shown to vanilla clients |

**Example — create a glowing red glass block:**

```
/bg define 70 Red Glow
/bg set 70 alltex 21
/bg set 70 draw 3
/bg set 70 bright 1
/bg set 70 light 1
/bg set 70 sound 6
/bg set 70 fallback 20
```

#### Block Inspection & Rollback

| Command | Description |
|---|---|
| `/bi` | Toggle inspect mode — click any block to see who changed it (up to 8 entries). Type `/bi` again to disable. |
| `/undo <player> [seconds]` | Revert all changes by a player in the current world within the time window (default: 300s) |
| `/purgehistory [days]` | Delete history entries older than N days (default: 7) |

#### Moderation

| Command | Description |
|---|---|
| `/op <password>` | Authenticate as operator |
| `/kick <player> [reason]` | Disconnect a player |
| `/ban <player> [reason]` | Ban a player (kicks if online, persists to `banned.json`) |
| `/unban <player>` | Remove a ban |
| `/banlist` | Show all banned players |
| `/hand <id> [lock]` | Set the block in your hand (0–255). Add `lock` to prevent switching. |

#### Server Settings

| Command | Description |
|---|---|
| `/set op <password>` | Change the operator password |
| `/set world <name> <password>` | Set or change a world's password |

## File Structure

```
goclassic
├── main.go                  # Server source
├── server.properties        # Configuration (auto-generated)
├── players.json             # Player positions & saved passwords
├── banned.json              # Ban list
├── customblocks.json        # Custom block definitions
├── global.json              # MCGalaxy block defs (auto-imported if present)
├── terrain.png              # Custom texture atlas (optional)
├── levels/
│   ├── hub.lvl              # Hub world (MCGalaxy format, gzipped)
│   ├── hub.portals.json     # Portal definitions for hub
│   ├── hub.welcome.txt      # Message shown when entering hub (optional)
│   ├── world1.lvl
│   ├── world1.welcome.txt      # Message shown when entering world1 (optional)
│   ├── world2.lvl
│   └── ...
└── blockhistory/
    ├── hub.json             # Block change log for hub
    ├── world1.json
    └── ...
```

### Level Format

Worlds use the MCGalaxy `.lvl` format (version 1873/1874) — gzip-compressed with a small header followed by raw block data. Levels created by MCGalaxy or ClassiCube can be dropped into the `levels/` directory and loaded on next startup.

### Welcome Messages

Create a file named `levels/<worldname>.welcome.txt` with one message per line. Each line is sent to the player as a chat message when they enter the world. Supports `&` color codes.

## Plugin System

The server has a built-in plugin interface with hooks for:

```go
type Plugin interface {
    Name() string
    OnLoad(s *Server)
    OnPlayerJoin(p *Player, w *World) EventResult
    OnPlayerLeave(p *Player, w *World)
    OnChat(p *Player, msg string) EventResult
    OnBlockChange(p *Player, x, y, z int16, blockID byte) EventResult
    OnPlayerMove(p *Player, x, y, z int16, yaw, pitch byte)
}
```

Returning `EventCancel` from `OnPlayerJoin`, `OnChat`, or `OnBlockChange` prevents the default action. Three plugins are built in:

- **HubProtection** — prevents building in the hub spawn building
- **Portals** — teleports players between worlds when they walk through portal regions
- **EventLogger** — logs joins and leaves with CPE capability info

## CPE Extensions

The server negotiates these [Classic Protocol Extensions](https://wiki.vg/Classic_Protocol_Extension):

| Extension | Version | Purpose |
|---|---|---|
| CustomBlocks | 1 | Enables block IDs 50–65 with fallback for vanilla clients |
| BlockDefinitions | 1 | Defines custom blocks 50–255 with textures, shapes, and physics |
| HeldBlock | 1 | Server can set the block in a player's hand (`/hand`) |
| EnvMapAspect | 1 | Sends a texture pack URL to clients |

Vanilla ClassiCube clients that don't support an extension gracefully fall back — custom blocks appear as their configured fallback block, and texture URLs are silently ignored.

## Building

Requires Go 1.21 or later. No external dependencies.

```bash
go build -o goclassic main.go
```

Cross-compile for other platforms:

```bash
GOOS=linux   GOARCH=amd64 go build -o goclassic-linux main.go
GOOS=windows GOARCH=amd64 go build -o goclassic.exe  main.go
GOOS=darwin  GOARCH=arm64 go build -o goclassic-mac   main.go
```

## License

This project is provided as-is under the MIT liscense.
