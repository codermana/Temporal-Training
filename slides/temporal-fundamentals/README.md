# Temporal Fundamentals: slide deck

Technical companion deck for the training. ~60 slides, ~3 hours including demos. Built with [Marp](https://marp.app).

## Preview

```bash
npm install                  # first time only
PORT=8081 npm run preview    # http://localhost:8081
```

From the repo root:

```bash
make slides-fundamentals     # runs on :8081
```

## Speaker notes

`slides.md` carries inline presenter notes as HTML comments after each slide.
Marp preserves them as speaker notes in PPTX exports and shows them in
presenter mode in the browser (press `p`).

## Export

```bash
npm run html       # dist/index.html
npm run pdf        # dist/slides.pdf
npm run pptx       # dist/slides.pptx
```

## Edit

- Content: `slides.md`
- Theme **source**: `../themes/base.scss` (shared by both decks)
- Images: `assets/images/`

### Theme (Sass)

`../themes/base.css` is **generated** from `base.scss`; don't edit it by hand.
The `preview`/`html`/`pdf`/`pptx` scripts recompile it automatically (a `pre*`
hook runs `npm run css`). To rebuild it on its own:

```bash
npm run css          # this deck
make slides-css      # from the repo root
```

The compile (`scripts/build-theme.mjs`) prepends Marp's `@import 'default';`
after Sass runs, because Dart Sass can't emit that bare import, and Marpit only inlines
its built-in default theme for exactly that form. `base.css` stays committed
because GitHub Pages and the Marp exports consume it directly.

## Deploy

CNAME ships set to `temporal-fundamentals.slides.codermana.com`. Adjust as needed.
