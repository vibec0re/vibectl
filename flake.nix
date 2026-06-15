# 🔥 VIBEC0RE NIX FLAKE - DEPLOY WITH STYLE! 💖
{
  description = "v1bectl - High-Performance Home Automation Control System";

  inputs = {
    nixpkgs = {
      type = "indirect";
      id = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        # 🔥 Use nixpkgs rustPlatform for CACHED builds! 💖
        commonBuildInputs = with pkgs; [
          openssl
        ] ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
          pkgs.darwin.apple_sdk.frameworks.Security
          pkgs.darwin.apple_sdk.frameworks.SystemConfiguration
        ];

        commonNativeBuildInputs = with pkgs; [
          pkg-config
        ];

        # 🔥 Server build - USES NIXPKGS CACHE! 💖
        v1bectl_server = pkgs.rustPlatform.buildRustPackage {
          pname = "v1bectl_server";
          version = "0.1.0";
          src = ./.;
          cargoLock.lockFile = ./Cargo.lock;

          buildInputs = commonBuildInputs;
          nativeBuildInputs = commonNativeBuildInputs;

          cargoBuildFlags = [ "-p" "v1bectl_server" ];
          cargoTestFlags = [ "-p" "v1bectl_server" ];

          doCheck = false; # Skip tests for faster builds
        };

        # 🔥 TUI build 💖
        v1bectl_tui = pkgs.rustPlatform.buildRustPackage {
          pname = "v1bectl_tui";
          version = "0.1.0";
          src = ./.;
          cargoLock.lockFile = ./Cargo.lock;

          buildInputs = commonBuildInputs;
          nativeBuildInputs = commonNativeBuildInputs;

          cargoBuildFlags = [ "-p" "v1bectl_tui" ];
          cargoTestFlags = [ "-p" "v1bectl_tui" ];

          doCheck = false;
        };

        # 🔥 CLI build 💖
        v1bectl_cli = pkgs.rustPlatform.buildRustPackage {
          pname = "v1bectl_cli";
          version = "0.1.0";
          src = ./.;
          cargoLock.lockFile = ./Cargo.lock;

          buildInputs = commonBuildInputs;
          nativeBuildInputs = commonNativeBuildInputs;

          cargoBuildFlags = [ "-p" "v1bectl_cli" ];
          cargoTestFlags = [ "-p" "v1bectl_cli" ];

          doCheck = false;
        };

        # 🔥 Dev shell with rust-overlay for nice tooling 💖
        devOverlays = [ (import rust-overlay) ];
        devPkgs = import nixpkgs {
          inherit system;
          overlays = devOverlays;
        };

        rustToolchain = devPkgs.rust-bin.stable.latest.default.override {
          extensions = [ "rust-src" "rust-analyzer" ];
          targets = [ "wasm32-unknown-unknown" ];
        };

        # 🔥 WEB UI BUILD (WASM) - uses rust-overlay for wasm target 💖
        v1bectl_web = devPkgs.stdenv.mkDerivation {
          pname = "v1bectl_web";
          version = "0.1.0";
          src = ./v1bectl_web;

          nativeBuildInputs = [
            rustToolchain
            devPkgs.trunk
            devPkgs.wasm-bindgen-cli
            devPkgs.binaryen
            devPkgs.sass
          ];

          buildPhase = ''
            export HOME=$TMPDIR
            export CARGO_HOME=$TMPDIR/.cargo
            trunk build --release
          '';

          installPhase = ''
            mkdir -p $out
            cp -r dist/* $out/
          '';
        };

      in
      {
        # 🔥 PACKAGES 💖
        packages = {
          inherit v1bectl_server v1bectl_tui v1bectl_cli v1bectl_web;

          server = v1bectl_server;
          tui = v1bectl_tui;
          cli = v1bectl_cli;
          web = v1bectl_web;

          default = v1bectl_server;
        };

        # 🔥 DEV SHELL - uses rust-overlay for nice DX 💖
        devShells.default = devPkgs.mkShell {
          packages = with devPkgs; [
            rustToolchain
            rust-analyzer
            cargo-watch
            cargo-edit
            trunk
            wasm-bindgen-cli
            openssl
            pkg-config
          ];

          shellHook = ''
            echo "🔥 VIBEC0RE Dev Shell Ready! 💖"
            echo ""
            echo "Commands:"
            echo "  cargo build     - Build all"
            echo "  cargo run -p v1bectl_server -- dummy"
            echo "  cargo run -p v1bectl_tui"
            echo "  cd v1bectl_web && trunk serve"
            echo ""
          '';
        };

        # 🔥 APPS 💖
        apps = {
          server = flake-utils.lib.mkApp {
            drv = v1bectl_server;
            name = "v1bectl_server";
          };
          tui = flake-utils.lib.mkApp {
            drv = v1bectl_tui;
            name = "v1bectl_tui";
          };
          cli = flake-utils.lib.mkApp {
            drv = v1bectl_cli;
            name = "v1bectl_cli";
          };
          default = self.apps.${system}.server;
        };
      }
    ) // {
      # 🔥 NIXOS MODULE 💖
      nixosModules.default = self.nixosModules.v1bectl;

      nixosModules.v1bectl = { config, lib, pkgs, ... }:
        let
          cfg = config.services.v1bectl;
        in
        {
          options.services.v1bectl = {
            enable = lib.mkEnableOption "v1bectl home automation server";

            package = lib.mkOption {
              type = lib.types.package;
              default = self.packages.${pkgs.system}.server;
              description = "v1bectl server package to use";
            };

            gateway = lib.mkOption {
              type = lib.types.enum [ "dummy" "dirigera" ];
              default = "dummy";
              description = "Gateway type to use";
            };

            dirigeraHost = lib.mkOption {
              type = lib.types.nullOr lib.types.str;
              default = null;
              description = "Dirigera gateway hostname";
            };

            accessTokenFile = lib.mkOption {
              type = lib.types.nullOr lib.types.path;
              default = null;
              description = "Path to file containing Dirigera access token";
            };

            port = lib.mkOption {
              type = lib.types.port;
              default = 31337;
              description = "Port for the v1bectl server";
            };

            openFirewall = lib.mkOption {
              type = lib.types.bool;
              default = false;
              description = "Open firewall for v1bectl";
            };
          };

          config = lib.mkIf cfg.enable {
            systemd.services.v1bectl = {
              description = "v1bectl Home Automation Server";
              wantedBy = [ "multi-user.target" ];
              after = [ "network.target" ];

              serviceConfig = {
                Type = "simple";
                DynamicUser = true;
                Restart = "always";
                RestartSec = 5;

                # Hardening
                NoNewPrivileges = true;
                ProtectSystem = "strict";
                ProtectHome = true;
                PrivateTmp = true;
              } // lib.optionalAttrs (cfg.accessTokenFile != null) {
                # 🔥 LOAD TOKEN SECURELY 💖
                LoadCredential = "token:${cfg.accessTokenFile}";
              };

              script =
                let
                  args = if cfg.gateway == "dirigera"
                    then "dirigera --host ${cfg.dirigeraHost} --port ${toString cfg.port}"
                    else "dummy --port ${toString cfg.port}";
                in ''
                  ${lib.optionalString (cfg.accessTokenFile != null) ''
                    export V1BECTL_ACCESS_TOKEN="$(cat $CREDENTIALS_DIRECTORY/token)"
                  ''}
                  exec ${cfg.package}/bin/v1bectl_server ${args}
                '';
            };

            networking.firewall.allowedTCPPorts = lib.mkIf cfg.openFirewall [ cfg.port ];
          };
        };

      # 🔥 NGINX WEB UI MODULE 💖
      nixosModules.web = { config, lib, pkgs, ... }:
        let
          cfg = config.services.v1bectl-web;
          webPkg = self.packages.${pkgs.system}.web;
        in
        {
          options.services.v1bectl-web = {
            enable = lib.mkEnableOption "v1bectl web UI with nginx";

            package = lib.mkOption {
              type = lib.types.package;
              default = webPkg;
              description = "v1bectl web UI package";
            };

            domain = lib.mkOption {
              type = lib.types.str;
              default = "localhost";
              description = "Domain name for the web UI";
            };

            serverAddr = lib.mkOption {
              type = lib.types.str;
              default = "127.0.0.1:31337";
              description = "Address of v1bectl server for WebSocket proxy";
            };

            enableSSL = lib.mkOption {
              type = lib.types.bool;
              default = false;
              description = "Enable SSL with ACME";
            };
          };

          config = lib.mkIf cfg.enable {
            services.nginx = {
              enable = true;

              # 🔥 WASM MIME TYPE 💖
              appendConfig = ''
                types {
                  application/wasm wasm;
                }
              '';

              virtualHosts.${cfg.domain} = {
                forceSSL = cfg.enableSSL;
                enableACME = cfg.enableSSL;

                # 🔥 USE PACKAGE DIRECTLY! 💖
                root = cfg.package;

                locations = {
                  "/" = {
                    index = "index.html";
                    tryFiles = "$uri $uri/ /index.html";
                  };

                  # 🔥 CACHE STATIC ASSETS 💖
                  "~* \\.(wasm|js|css|png|svg|ico)$" = {
                    extraConfig = ''
                      expires 7d;
                      add_header Cache-Control "public, immutable";
                    '';
                  };

                  # 🔥 WEBSOCKET PROXY TO SERVER 💖
                  "/ws" = {
                    proxyPass = "http://${cfg.serverAddr}";
                    proxyWebsockets = true;
                    extraConfig = ''
                      proxy_read_timeout 86400;
                      proxy_send_timeout 86400;
                      proxy_connect_timeout 60;
                    '';
                  };

                  # 🔥 API PROXY 💖
                  "/api" = {
                    proxyPass = "http://${cfg.serverAddr}";
                  };
                };

                extraConfig = ''
                  gzip on;
                  gzip_types application/javascript application/wasm text/css text/html;
                  gzip_min_length 1000;
                '';
              };
            };
          };
        };

      # 🔥 OVERLAY 💖
      overlays.default = final: prev: {
        v1bectl_server = self.packages.${prev.system}.server;
        v1bectl_tui = self.packages.${prev.system}.tui;
        v1bectl_cli = self.packages.${prev.system}.cli;
        v1bectl_web = self.packages.${prev.system}.web;
      };
    };
}
