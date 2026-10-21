(function () {
  "use strict";

  var reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

  // Hero carousel: crossfades through the app's screens once so a visitor sees the product
  // without clicking, then stops. It also stops the moment someone uses a tab, the Pause
  // button, the keyboard, or hovers or focuses inside it, and never advances on its own with
  // reduced motion. Inactive slides are hidden from screen readers.
  var slides = Array.prototype.slice.call(document.querySelectorAll(".slide"));
  var tabs = Array.prototype.slice.call(document.querySelectorAll(".tab"));
  var stage = document.getElementById("stage");
  var wrap = document.querySelector(".stage-wrap");
  var pauseBtn = document.getElementById("pause");
  var index = 0;
  var shown = 1;            // how many screens autoplay has shown, to stop after one full loop
  var userPaused = reduce;
  var inside = false;
  var timer = null;

  function show(i, focusTab) {
    index = (i + slides.length) % slides.length;
    slides.forEach(function (s, n) {
      var on = n === index;
      s.classList.toggle("is-active", on);
      s.setAttribute("aria-hidden", on ? "false" : "true");
    });
    tabs.forEach(function (t, n) {
      var on = n === index;
      t.setAttribute("aria-selected", on ? "true" : "false");
      t.setAttribute("tabindex", on ? "0" : "-1");
    });
    if (focusTab) tabs[index].focus();
  }

  function setPaused(p) {
    userPaused = p;
    if (pauseBtn) {
      pauseBtn.textContent = p ? "Play" : "Pause";
      pauseBtn.setAttribute("aria-pressed", p ? "true" : "false");
    }
    schedule();
  }

  function schedule() {
    clearTimeout(timer);
    if (userPaused || inside || document.hidden || shown >= slides.length) return;
    timer = setTimeout(function () { show(index + 1); shown += 1; schedule(); }, 4800);
  }

  tabs.forEach(function (t, n) {
    t.addEventListener("click", function () { show(n); setPaused(true); });
    t.addEventListener("keydown", function (e) {
      var to = null;
      if (e.key === "ArrowRight") to = n + 1;
      else if (e.key === "ArrowLeft") to = n - 1;
      else if (e.key === "Home") to = 0;
      else if (e.key === "End") to = tabs.length - 1;
      if (to === null) return;
      e.preventDefault();
      show(to, true);
      setPaused(true);
    });
  });

  if (wrap) {
    wrap.addEventListener("mouseenter", function () { inside = true; schedule(); });
    wrap.addEventListener("mouseleave", function () { inside = false; schedule(); });
    wrap.addEventListener("focusin", function () { inside = true; schedule(); });
    wrap.addEventListener("focusout", function () { inside = false; schedule(); });
  }
  if (pauseBtn) {
    pauseBtn.textContent = reduce ? "Play" : "Pause";
    pauseBtn.setAttribute("aria-pressed", reduce ? "true" : "false");
    pauseBtn.addEventListener("click", function () {
      if (userPaused && shown >= slides.length) shown = 1; // Play after a finished loop runs another loop
      setPaused(!userPaused);
    });
  }
  document.addEventListener("visibilitychange", schedule);
  schedule();

  // Scroll reveals: the bento tiles and the setup steps fade up once as they come into view.
  // IntersectionObserver only, no scroll listeners.
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
