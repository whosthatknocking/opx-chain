# Design Specification: Options Chain Viewer

## 1. Overview
The viewer uses an institutional, ledger-inspired visual system optimized for dense financial data. The goal is clarity, structural hierarchy, and fast scanning, without decorative marketing copy or non-functional chrome.

## 2. Visual Principles
* **Precision over Decoration:** Borders, spacing, and typography define hierarchy.
* **Data Density:** The dataset table remains the primary surface and should stay compact enough for active analysis.
* **Structural Contrast:** Sections are separated with 1px borders and subtle surface shifts instead of shadows.
* **Functional UI Copy:** Titles, tabs, and controls should describe what the user can do, not provide branding slogans.

## 3. Visual Language

### A. Color Palette
The viewer supports two modes with the same core identity: slate surfaces, high-contrast text, emerald positive states, blue neutral states, and red negative states.

#### Light Mode
* **Surface Primary:** `#FFFFFF`
* **Surface Secondary / Page Ground:** `#F8FAFC`
* **Text Primary:** `#0F172A`
* **Text Secondary:** `#475569`
* **Borders:** `#E2E8F0`

#### Dark Mode
* **Surface Primary:** `#1E293B`
* **Surface Secondary / Page Ground:** `#0F172A`
* **Text Primary:** `#F1F5F9`
* **Text Secondary:** `#94A3B8`
* **Borders:** `#334155`

#### Functional Colors
* **Positive:** `#10B981`
* **Negative:** `#EF4444`
* **Neutral / Active:** `#3B82F6`

### B. Typography
* **Primary Font:** **Manrope**
* **Section Titles:** bold, compact, high-contrast
* **Table Headers:** uppercase with tight tracking
* **Supporting Labels:** small, muted, uppercase when used as metadata

### C. Shape & Form
* **Corner Radius:** `4px`
* **Shadows:** none or effectively none
* **Borders:** always preferred over elevation for separation

## 4. Layout

### A. Header
The viewer uses a single functional header instead of a persistent sidebar.

Header contents:
* Viewer title: `Options Chain Viewer`
* Primary tabs: `Dataset`, `Overview`, `Chain View`, `Reference`
* Dataset selector: current CSV file chooser
* Theme toggle: `Light` / `Dark`

### B. Main Surfaces
* **Dataset tab:** toolbar, freshness cards, dataset cards, options table, pagination
* **Overview tab:** ticker summary cards and opportunity cards
* **Chain View tab:** one chart card per row, stacked vertically, using the same panel language as the rest of the viewer
* **Reference tab:** rendered documentation/readme content

Dataset tab behavior:
* The dataset pane is constrained to the viewport rather than growing indefinitely.
* The table scrolls inside its card.
* The toolbar stays accessible at the top of the dataset pane.
* Pagination remains visible at the bottom of the table card.

Chain View behavior:
* Charts are derived client-side from the currently selected CSV snapshot.
* The tab provides per-underlying and per-expiration selectors plus option-side and x-axis controls.
* Each chart card keeps its title, note, legend, and plot in a single vertical flow.
* Chart marks support hover inspection and click-through into the existing row-detail modal.

### C. Responsive Behavior
* Desktop-first layout
* Header controls wrap into stacked rows on smaller viewports
* No collapsed icon-only sidebar behavior remains in scope

## 5. Key Components

### A. Dataset Table
* Sticky header
* Uppercase column labels with strong tracking
* Approximate 48px row height
* Subtle hover state
* Pill-style indicators for qualitative risk/status values when applicable
* Internal scrolling region so paging controls remain reachable without scrolling past the entire page

### B. Cards
* Flat 1px bordered surfaces
* Used for freshness summaries, dataset metadata, ticker summaries, opportunities, and row details
* Metrics should remain easy to compare at a glance

### C. Controls
* Tabs, selectors, filter controls, modal actions, and pager buttons use the same 4px radius and border-led styling
* Theme toggle label must remain concise: `Light` / `Dark`

## 6. Naming Conventions
Use the current functional UI labels:
* Title: `Options Chain Viewer`
* Tab 1: `Dataset`
* Tab 2: `Overview`
* Tab 3: `Chain View`
* Tab 4: `Reference`

Avoid reintroducing older names such as `Ledger`, `Portfolio`, or `Equity Ledger` unless the UI changes again.

## 7. Implementation Notes
* Maintain contrast of at least 4.5:1 for readable data text.
* Preserve dark/light mode switching in the header.
* Keep visual updates aligned with the existing viewer behavior rather than introducing extra presentation-only content.
* Keep dataset navigation controls visible during analysis, especially `Rows Per Page`, `Previous`, and `Next`.
