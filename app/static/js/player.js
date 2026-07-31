'use strict';

(function () {
  var audio = document.getElementById('global-audio');
  var player = document.getElementById('global-player');
  if (!audio || !player) return;

  var listeners = new AbortController();
  var queue = [];
  var index = -1;
  var retriedTranscode = false;
  var title = player.querySelector('[data-player-title]');
  var artist = player.querySelector('[data-player-artist]');
  var art = player.querySelector('[data-player-art]');
  var toggle = player.querySelector('[data-player-toggle]');
  var previous = player.querySelector('[data-player-previous]');
  var next = player.querySelector('[data-player-next]');
  var seek = player.querySelector('[data-player-seek]');
  var volume = player.querySelector('[data-player-volume]');
  var mute = player.querySelector('[data-player-mute]');
  var currentTime = player.querySelector('[data-player-current-time]');
  var duration = player.querySelector('[data-player-duration]');
  var status = player.querySelector('[data-player-status]');

  function formatTime(value) {
    if (!Number.isFinite(value) || value < 0) return '0:00';
    var minutes = Math.floor(value / 60);
    var seconds = Math.floor(value % 60).toString().padStart(2, '0');
    return minutes + ':' + seconds;
  }

  function setStatus(message, state) {
    status.textContent = message;
    player.dataset.playerState = state;
  }

  function metadataFrom(button) {
    if (!button.dataset.playUrl || button.disabled || button.getAttribute('aria-disabled') === 'true') return null;
    return {
      url: button.dataset.playUrl,
      id: button.getAttribute('data-track-id') || button.dataset.playUrl,
      title: button.dataset.trackTitle || 'Unknown track',
      artist: button.dataset.trackArtist || 'Unknown artist',
      artwork: button.dataset.trackArtwork || '',
    };
  }

  function queueFor(button) {
    var scope = button.closest('[data-play-queue]') || document.querySelector('[data-page-region]');
    var items = Array.from(scope.querySelectorAll('[data-play-url]')).map(metadataFrom).filter(Boolean);
    var selected = metadataFrom(button);
    var selectedIndex = selected ? items.findIndex(function (item) { return item.id === selected.id; }) : -1;
    return { items: items, selectedIndex: selectedIndex };
  }

  function updateButtons() {
    var active = index >= 0 && index < queue.length;
    toggle.disabled = !active;
    seek.disabled = !active;
    previous.disabled = !active || index <= 0;
    next.disabled = !active || index >= queue.length - 1;
    toggle.textContent = audio.paused ? '▶' : '⏸';
    toggle.setAttribute('aria-label', audio.paused ? 'Play' : 'Pause');
  }

  function updateMediaSession(item) {
    if (!('mediaSession' in navigator)) return;
    navigator.mediaSession.metadata = new MediaMetadata({
      title: item.title,
      artist: item.artist,
      artwork: item.artwork ? [{ src: item.artwork }] : [],
    });
  }

  function renderItem(item) {
    title.textContent = item.title;
    artist.textContent = item.artist;
    art.replaceChildren();
    if (item.artwork) {
      var image = document.createElement('img');
      image.src = item.artwork;
      image.alt = '';
      art.append(image);
    } else {
      var placeholder = document.createElement('span');
      placeholder.textContent = '♪';
      art.append(placeholder);
    }
    updateMediaSession(item);
  }

  function playAt(nextIndex) {
    if (nextIndex < 0 || nextIndex >= queue.length) return;
    index = nextIndex;
    retriedTranscode = false;
    var item = queue[index];
    renderItem(item);
    audio.src = item.url;
    audio.load();
    setStatus('Loading ' + item.title, 'loading');
    updateButtons();
    audio.play().catch(function () {
      setStatus('Playback needs your permission', 'paused');
      updateButtons();
    });
  }

  function transcodeUrl(source) {
    var url = new URL(source, window.location.href);
    url.searchParams.set('transcode', 'mp3');
    return url.pathname + url.search;
  }

  function isTypingTarget(target) {
    if (!target) return false;
    if (target.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(target.tagName)) return true;
    return Boolean(target.closest && target.closest('button, a, [role="button"]'));
  }

  document.addEventListener('click', function (event) {
    var button = event.target.closest('[data-play-url]');
    if (!button) return;
    event.preventDefault();
    var nextQueue = queueFor(button);
    if (nextQueue.selectedIndex < 0) return;
    queue = nextQueue.items;
    playAt(nextQueue.selectedIndex);
  }, { signal: listeners.signal });

  toggle.addEventListener('click', function () {
    if (audio.paused) audio.play().catch(function () { setStatus('Unable to play this track', 'error'); });
    else audio.pause();
  }, { signal: listeners.signal });
  previous.addEventListener('click', function () { playAt(index - 1); }, { signal: listeners.signal });
  next.addEventListener('click', function () { playAt(index + 1); }, { signal: listeners.signal });
  seek.addEventListener('input', function () {
    if (Number.isFinite(audio.duration)) audio.currentTime = (Number(seek.value) / 1000) * audio.duration;
  }, { signal: listeners.signal });
  volume.addEventListener('input', function () {
    audio.volume = Number(volume.value);
    audio.muted = false;
  }, { signal: listeners.signal });
  mute.addEventListener('click', function () { audio.muted = !audio.muted; }, { signal: listeners.signal });

  audio.addEventListener('loadedmetadata', function () {
    duration.textContent = formatTime(audio.duration);
    seek.disabled = false;
    updateButtons();
  }, { signal: listeners.signal });
  audio.addEventListener('timeupdate', function () {
    currentTime.textContent = formatTime(audio.currentTime);
    seek.value = Number.isFinite(audio.duration) && audio.duration > 0 ? String(Math.round((audio.currentTime / audio.duration) * 1000)) : '0';
  }, { signal: listeners.signal });
  audio.addEventListener('play', function () { updateButtons(); }, { signal: listeners.signal });
  audio.addEventListener('pause', function () {
    if (!audio.ended) setStatus('Playback paused', 'paused');
    updateButtons();
  }, { signal: listeners.signal });
  audio.addEventListener('waiting', function () { setStatus('Loading audio', 'loading'); }, { signal: listeners.signal });
  audio.addEventListener('playing', function () {
    setStatus('Playing ' + queue[index].title, 'playing');
    updateButtons();
  }, { signal: listeners.signal });
  audio.addEventListener('ended', function () {
    if (index + 1 < queue.length) playAt(index + 1);
    else { setStatus('Queue finished', 'paused'); updateButtons(); }
  }, { signal: listeners.signal });
  audio.addEventListener('volumechange', function () {
    volume.value = String(audio.muted ? 0 : audio.volume);
    mute.textContent = audio.muted || audio.volume === 0 ? '🔇' : '🔊';
    mute.setAttribute('aria-label', audio.muted ? 'Unmute' : 'Mute');
  }, { signal: listeners.signal });
  audio.addEventListener('error', function () {
    if (index < 0) return;
    if (!retriedTranscode && !audio.src.includes('transcode=mp3')) {
      retriedTranscode = true;
      audio.src = transcodeUrl(queue[index].url); // ?transcode=mp3 browser-compatible retry
      audio.load();
      setStatus('Retrying with browser-compatible audio', 'loading');
      audio.play().catch(function () { setStatus('Audio is unavailable', 'error'); });
      return;
    }
    setStatus('Audio is unavailable', 'error');
    updateButtons();
  }, { signal: listeners.signal });

  document.addEventListener('keydown', function (event) {
    if (isTypingTarget(event.target) || event.metaKey || event.ctrlKey || event.altKey) return;
    if (event.code === 'Space' && index >= 0) {
      event.preventDefault();
      toggle.click();
    } else if (event.code === 'ArrowRight' && index >= 0 && Number.isFinite(audio.duration)) {
      audio.currentTime = Math.min(audio.duration, audio.currentTime + 10);
    } else if (event.code === 'ArrowLeft' && index >= 0) {
      audio.currentTime = Math.max(0, audio.currentTime - 10);
    }
  }, { signal: listeners.signal });

  if ('mediaSession' in navigator) {
    navigator.mediaSession.setActionHandler('play', function () { audio.play(); });
    navigator.mediaSession.setActionHandler('pause', function () { audio.pause(); });
    navigator.mediaSession.setActionHandler('previoustrack', function () { playAt(index - 1); });
    navigator.mediaSession.setActionHandler('nexttrack', function () { playAt(index + 1); });
    navigator.mediaSession.setActionHandler('seekto', function (details) {
      if (details.seekTime != null) audio.currentTime = details.seekTime;
    });
  }

  document.addEventListener('audiohoard:page-dispose', function () {
    // The player is global: only transient page hooks are discarded by navigation.
  }, { signal: listeners.signal });
  updateButtons();
}());
