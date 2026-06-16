#!/usr/bin/env node
// Compile the shared Marp theme: slides/themes/base.scss -> slides/themes/base.css
//
// Why a script instead of a plain `sass` invocation:
//   Marpit only inlines its built-in `default` theme when the theme file holds a
//   bare `@import 'default';`. Dart Sass cannot emit that form - it tries to
//   resolve `'default'` as a stylesheet at compile time and errors. The two
//   Sass-passthrough forms (`@import url('default')`, `@import 'default.css'`)
//   compile fine but Marpit does NOT recognise them, so the default theme is
//   never inlined and the deck renders unstyled.
//
//   So we keep `@import 'default';` (plus the @theme banner) OUT of base.scss and
//   prepend it here, after Sass has done its work. base.scss starts straight at
//   the Google Fonts `@import url(...)`, which Sass passes through untouched.
//
// base.css is a generated artifact - edit base.scss, then run `npm run css`
// (from either deck) or `make slides-css`. The compiled file is committed because
// GitHub Pages / the Marp exports consume it directly with no build step.

import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';

// Resolve `sass` from the current working directory's node_modules (each deck
// installs it as a devDependency), not from this script's location.
const require = createRequire(join(process.cwd(), 'noop.js'));
let sass;
try {
  sass = require('sass');
} catch {
  console.error(
    'Could not find the `sass` package. Run `npm install` in a deck directory\n' +
      '(slides/why-temporal or slides/temporal-fundamentals) first, or use `make slides-css`.',
  );
  process.exit(1);
}

const themesDir = join(dirname(fileURLToPath(import.meta.url)), '..', 'slides', 'themes');
const src = join(themesDir, 'base.scss');
const out = join(themesDir, 'base.css');

// Injected verbatim ahead of the Sass output. `@import 'default';` MUST stay
// here (see the note above). Keep the @theme banner in sync with base.scss's
// expectations - the `theme: base` front-matter in each deck refers to it.
const header = `/*!
 * @theme base
 * @auto-scaling true
 *
 * GENERATED FILE - do not edit. Source: themes/base.scss
 * Rebuild with \`npm run css\` (either deck) or \`make slides-css\`.
 */

@import 'default';
`;

// charset: false -> never emit a leading `@charset` rule. The output is UTF-8
// and `@import 'default';` is prepended ahead of it, so a `@charset` would land
// mid-stylesheet (invalid) and the original base.css never had one.
const { css } = sass.compile(src, { style: 'expanded', charset: false, loadPaths: [themesDir] });
writeFileSync(out, `${header}\n${css}\n`);
console.log(`Built ${out} from base.scss`);
