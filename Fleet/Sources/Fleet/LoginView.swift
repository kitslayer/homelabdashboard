import SwiftUI

struct LoginView: View {
    @EnvironmentObject var session: Session
    @State private var token: String = ""
    @State private var primary: String = ""
    @State private var fallback: String = ""
    @State private var busy = false
    @State private var error: String?

    var body: some View {
        ZStack {
            Theme.bg.ignoresSafeArea()
            ScrollView {
                VStack(alignment: .leading, spacing: 16) {
                    Text("Fleet")
                        .font(.system(size: 38, weight: .bold))
                        .padding(.top, 60)
                    Text("Sign in with the admin token to view your homelab. Tokens are stored locally on this device only.")
                        .foregroundStyle(.secondary)
                        .padding(.bottom, 4)

                    VStack(alignment: .leading, spacing: 8) {
                        Text("Admin token").font(.caption).foregroundStyle(.secondary)
                        SecureField("paste here", text: $token)
                            .textContentType(.password)
                            .autocorrectionDisabled()
                            .textInputAutocapitalization(.never)
                            .padding(10)
                            .background(RoundedRectangle(cornerRadius: 10).fill(Theme.panel))
                    }

                    VStack(alignment: .leading, spacing: 8) {
                        Text("Primary server (LAN / Tailscale)").font(.caption).foregroundStyle(.secondary)
                        TextField("http://192.0.2.10", text: $primary)
                            .autocorrectionDisabled()
                            .textInputAutocapitalization(.never)
                            .keyboardType(.URL)
                            .padding(10)
                            .background(RoundedRectangle(cornerRadius: 10).fill(Theme.panel))
                    }

                    VStack(alignment: .leading, spacing: 8) {
                        Text("Fallback (public)").font(.caption).foregroundStyle(.secondary)
                        TextField("https://milescoviello.com", text: $fallback)
                            .autocorrectionDisabled()
                            .textInputAutocapitalization(.never)
                            .keyboardType(.URL)
                            .padding(10)
                            .background(RoundedRectangle(cornerRadius: 10).fill(Theme.panel))
                    }

                    Button {
                        Task { await signIn() }
                    } label: {
                        ZStack {
                            if busy {
                                ProgressView().tint(.white)
                            } else {
                                Text("Sign in").bold()
                            }
                        }
                        .frame(maxWidth: .infinity)
                        .padding(.vertical, 12)
                        .background(RoundedRectangle(cornerRadius: 10).fill(Theme.accent))
                        .foregroundStyle(.white)
                    }
                    .disabled(busy)
                    .padding(.top, 8)

                    if let error {
                        Text(error)
                            .foregroundStyle(Theme.crit)
                            .font(.footnote)
                            .multilineTextAlignment(.leading)
                    }
                }
                .padding(.horizontal, 28)
            }
        }
        .onAppear {
            primary = session.primaryServer
            fallback = session.fallbackServer
        }
    }

    private func signIn() async {
        busy = true
        defer { busy = false }
        session.primaryServer = primary
        session.fallbackServer = fallback
        let ok = await session.signIn(token)
        if !ok {
            error = session.statusError ?? "Token rejected"
        }
    }
}
