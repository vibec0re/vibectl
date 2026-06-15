# 🔥 VIBEC0RE NixOS Deployment Guide 💖

Deploy v1bectl on NixOS - works with classic config OR flakes!

## Option 1: Classic NixOS (no flakes)

### Step 1: Add flake input to configuration.nix

```nix
# /etc/nixos/configuration.nix
{ config, pkgs, lib, ... }:

let
  # 🔥 Fetch v1bectl flake 💖
  v1bectl = builtins.getFlake "github:vibec0re/vibectl";
  v1bectl-pkgs = v1bectl.packages.${pkgs.system};
  v1bectl-modules = v1bectl.nixosModules;
in
{
  imports = [
    ./hardware-configuration.nix
    v1bectl-modules.v1bectl
    v1bectl-modules.web
  ];

  # 🔥 Enable flakes (needed for builtins.getFlake) 💖
  nix.settings.experimental-features = [ "nix-command" "flakes" ];

  # 🔥 v1bectl Server 💖
  services.v1bectl = {
    enable = true;
    gateway = "dirigera";           # or "dummy" for testing
    dirigeraHost = "gw2-xxx.local"; # your Dirigera gateway
    port = 31337;
    openFirewall = true;
  };

  # 🔥 v1bectl Web UI 💖
  services.v1bectl-web = {
    enable = true;
    domain = "nest.local";          # your domain
    # enableSSL = true;             # enable for HTTPS + ACME
  };

  # ... rest of your config
}
```

### Step 2: Rebuild

```bash
sudo nixos-rebuild switch
```

---

## Option 2: Flake-based NixOS

### Step 1: Create/update flake.nix

```nix
# /etc/nixos/flake.nix
{
  description = "My NixOS Configuration";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

    # 🔥 Add v1bectl 💖
    v1bectl.url = "github:vibec0re/vibectl";
  };

  outputs = { self, nixpkgs, v1bectl, ... }: {
    nixosConfigurations.myhost = nixpkgs.lib.nixosSystem {
      system = "x86_64-linux";
      modules = [
        ./configuration.nix

        # 🔥 Import v1bectl modules 💖
        v1bectl.nixosModules.v1bectl
        v1bectl.nixosModules.web
      ];
    };
  };
}
```

### Step 2: Update configuration.nix

```nix
# /etc/nixos/configuration.nix
{ config, pkgs, ... }:

{
  imports = [ ./hardware-configuration.nix ];

  # 🔥 v1bectl Server 💖
  services.v1bectl = {
    enable = true;
    gateway = "dirigera";
    dirigeraHost = "gw2-xxx.local";
    openFirewall = true;
  };

  # 🔥 v1bectl Web UI 💖
  services.v1bectl-web = {
    enable = true;
    domain = "nest.local";
  };

  # ... rest of your config
}
```

### Step 3: Rebuild

```bash
sudo nixos-rebuild switch --flake /etc/nixos#myhost
```

---

## Option 3: Hybrid (classic config + optional flake overlay)

Keep your classic setup, just add packages via overlay:

```nix
# /etc/nixos/configuration.nix
{ config, pkgs, lib, ... }:

let
  # 🔥 Optional: fetch v1bectl if flakes enabled 💖
  v1bectl = if builtins.hasAttr "getFlake" builtins
    then builtins.getFlake "github:vibec0re/vibectl"
    else null;
in
{
  imports = [ ./hardware-configuration.nix ]
    ++ lib.optionals (v1bectl != null) [
      v1bectl.nixosModules.v1bectl
      v1bectl.nixosModules.web
    ];

  # Enable flakes
  nix.settings.experimental-features = [ "nix-command" "flakes" ];

  # 🔥 Only enable if flake available 💖
  services.v1bectl = lib.mkIf (v1bectl != null) {
    enable = true;
    gateway = "dirigera";
    dirigeraHost = "gw2-xxx.local";
    openFirewall = true;
  };

  services.v1bectl-web = lib.mkIf (v1bectl != null) {
    enable = true;
    domain = "nest.local";
  };
}
```

---

## Configuration Options

### services.v1bectl

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enable` | bool | `false` | Enable v1bectl server |
| `gateway` | enum | `"dummy"` | `"dummy"` or `"dirigera"` |
| `dirigeraHost` | string | `null` | Dirigera gateway hostname |
| `accessTokenFile` | path | `null` | Path to token file |
| `port` | int | `31337` | Server port |
| `openFirewall` | bool | `false` | Open firewall port |

### services.v1bectl-web

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enable` | bool | `false` | Enable web UI |
| `domain` | string | `"localhost"` | Domain name |
| `serverAddr` | string | `"127.0.0.1:31337"` | Backend address |
| `enableSSL` | bool | `false` | Enable HTTPS + ACME |

---

## SSL with Let's Encrypt

```nix
{
  services.v1bectl-web = {
    enable = true;
    domain = "nest.example.com";
    enableSSL = true;
  };

  # Required for ACME
  security.acme = {
    acceptTerms = true;
    defaults.email = "you@example.com";
  };
}
```

---

## Local DNS (optional)

Add local domain to hosts:

```nix
{
  networking.hosts = {
    "127.0.0.1" = [ "nest.local" ];
  };
}
```

Or use Avahi for `.local` domains:

```nix
{
  services.avahi = {
    enable = true;
    nssmdns4 = true;
    publish = {
      enable = true;
      addresses = true;
    };
  };
}
```

---

## Troubleshooting

### Check service status
```bash
systemctl status v1bectl
journalctl -u v1bectl -f
```

### Check nginx
```bash
systemctl status nginx
nginx -t
```

### Test WebSocket
```bash
websocat ws://localhost:31337/ws
```

### Rebuild with verbose
```bash
sudo nixos-rebuild switch --show-trace
```

---

🔥 **LET'S FUCKING GOOOOO!** 💖
