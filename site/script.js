(function () {
  "use strict";

  var reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  // Hero carousel: crossfades through the app's screens so a visitor sees the product
  // without clicking. Pauses on hover and focus, when the tab is hidden, and has a
  // visible Pause button. With reduced motion it never advances on its own.
  var slides = Array.prototype.slice.call(document.querySelectorAll(".slide"));
  var tabs = Array.prototype.slice.call(document.querySelectorAll(".tab[role=tab]"));
  var stage = document.getElementById("stage");
  var pauseBtn = document.getElementById("pause");
  var index = 0;
  var userPaused = reduce;
  var hovering = false;
  var timer = null;

  function show(i) {
    index = (i + slides.length) % slides.length;
    slides.forEach(function (s, n) { s.classList.toggle("is-active", n === index); });
    tabs.forEach(function (t, n) { t.setAttribute("aria-selected", n === index ? "true" : "false"); });
  }

  function schedule() {
    clearTimeout(timer);
    if (userPaused || hovering || document.hidden) return;
    timer = setTimeout(function () { show(index + 1); schedule(); }, 4800);
  }

  tabs.forEach(function (t, n) {
    t.addEventListener("click", function () { show(n); schedule(); });
    t.addEventListener("keydown", function (e) {
      if (e.key === "ArrowRight") { tabs[(n + 1) % tabs.length].focus(); show(n + 1); schedule(); e.preventDefault(); }
      if (e.key === "ArrowLeft") { tabs[(n - 1 + tabs.length) % tabs.length].focus(); show(n - 1); schedule(); e.preventDefault(); }
    });
  });

  if (stage) {
    stage.addEventListener("mouseenter", function () { hovering = true; schedule(); });
    stage.addEventListener("mouseleave", function () { hovering = false; schedule(); });
  }
  if (pauseBtn) {
    pauseBtn.textContent = reduce ? "Play" : "Pause";
    pauseBtn.setAttribute("aria-pressed", reduce ? "true" : "false");
    pauseBtn.addEventListener("click", function () {
      userPaused = !userPaused;
      pauseBtn.textContent = userPaused ? "Play" : "Pause";
      pauseBtn.setAttribute("aria-pressed", userPaused ? "true" : "false");
      schedule();
    });
  }
  document.addEventListener("visibilitychange", schedule);
  schedule();

  // Scroll reveals: content fades up once as it comes into view. IntersectionObserver only,
  // no scroll listeners.
  var items = document.querySelectorAll(".reveal");
  if ("IntersectionObserver" in window && !reduce) {
    var io = new IntersectionObserver(function (entries) {
      entries.forEach(function (e) {
        if (e.isIntersecting) { e.target.classList.add("in"); io.unobserve(e.target); }
      });
    }, { threshold: 0.12, rootMargin: "0px 0px -6% 0px" });
    items.forEach(function (el) { io.observe(el); });
  } else {
    items.forEach(function (el) { el.classList.add("in"); });
  }
})();
