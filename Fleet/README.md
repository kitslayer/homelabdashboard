# Fleet — iOS app

SwiftUI client for the homelab-stats fleet backend. Built with
[xtool](https://xtool.sh) so you don't need a Mac to compile.

## Build prerequisites

1. **Swift 6.2+** — installed via `swiftly` (already on this laptop at
   `~/.local/share/swiftly`).
2. **xtool** at `/usr/local/bin/xtool` (already installed).
3. **Xcode 26 .xip** — you must download this once from
   <https://download.developer.apple.com/Developer_Tools/Xcode_26.0.1/Xcode_26.0.1_Apple_silicon.xip>
   in a browser (Apple requires auth). Save it anywhere; xtool will
   re-package it into a Swift SDK on first run.
4. **usbmuxd + libimobiledevice** — for installing the IPA on a tethered
   iPhone (`sudo emerge libimobiledevice usbmuxd` on Gentoo).
5. **Throwaway Apple ID** — used for the free dev cert. xtool stores the
   credentials locally only.

## First-time setup

```
cd /home/miles/Stats/Fleet
. ~/.local/share/swiftly/env.sh
xtool setup
# answer "1" (password auth, free Apple ID)
# point it at the Xcode.xip you downloaded
```

`xtool setup` will:

* Authenticate the throwaway Apple ID against Apple Developer Services
* Extract the iOS SDK from Xcode.xip (~10 minutes)
* Install the Swift SDK so SwiftPM can target `arm64-apple-ios`

## Build & deploy to a phone

Plug your iPhone in over USB and unlock it, then:

```
cd /home/miles/Stats/Fleet
xtool dev
```

`xtool dev` builds the SwiftPM project for iOS, generates an IPA, signs
it with the dev cert, and installs it onto the connected device.
Re-running `xtool dev` after edits incrementally rebuilds and reinstalls.

To produce just an IPA without installing:

```
xtool dev build
ls .build/xtool/*.ipa
```

## Notes on free dev certs

* The signed app expires 7 days after install. Re-run `xtool dev` to
  refresh it.
* APNs is not reliable with free dev certs, so the app uses local
  notifications fired from a background refresh loop. The notification
  permission is requested on first launch.
* Bundle ID is `com.milescoviello.Fleet` (set in `xtool.yml`). If you
  switch Apple IDs, regenerate the dev cert before reinstalling.

## Configuration

The app stores its admin token + server URLs in `UserDefaults` on the
device. Server defaults match the cluster:

* Primary: `http://<LAN_VIP>` (homelab-stats-lb MetalLB IP)
* Fallback: `https://stats.milescoviello.com`

Edit them under Settings if you move things.

## Project layout

```
Package.swift           — SwiftPM target definition
xtool.yml               — xtool app metadata (bundle id, version)
Sources/Fleet/
  FleetApp.swift        — @main, root tab view
  Theme.swift           — colors + panel modifier
  Models.swift          — host / sample / alert codables
  Session.swift         — token + server URL state
  Network.swift         — server failover, JSON wrappers
  FleetStore.swift      — polling loop + notification dispatch
  LoginView.swift       — admin token entry
  DashboardView.swift   — host grid
  HostDetailView.swift  — per-host live + history (Swift Charts)
  MapView.swift         — topology / geographic / floorplan
  AlertsView.swift      — open + cleared alerts
  SettingsView.swift    — servers + status + sign out
```
