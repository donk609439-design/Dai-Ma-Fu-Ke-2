{pkgs}: {
  deps = [
    pkgs.rustc
    pkgs.cargo
    pkgs.libiconv
    pkgs.libxcrypt
    pkgs.pkg-config
    pkgs.openssl
    pkgs.postgresql
  ];
}
