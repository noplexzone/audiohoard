document.getElementById("setup-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const form = event.target;
  const data = Object.fromEntries(new FormData(form));
  const credentials = {username: data.username, password: data.password};
  const providerFields = [
    "slskd_url", "slskd_api_key", "prowlarr_url", "prowlarr_api_key",
    "sabnzbd_url", "sabnzbd_api_key", "ytdlp_cookies_file",
    "tidal_config_path", "tidal_session_path", "tidal_quality",
    "musicbrainz_contact", "acoustid_api_key", "library_root", "staging_root",
    "naming_template",
  ];
  const providerSettings = {};
  providerFields.forEach((field) => {
    if (data[field]) providerSettings[field] = data[field];
  });
  const payload = Object.keys(providerSettings).length
    ? {...credentials, provider_settings: providerSettings}
    : credentials;
  const response = await fetch("/api/auth/setup", {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    window.alert(error.detail || "Setup failed");
    return;
  }
  window.location.assign("/");
});
