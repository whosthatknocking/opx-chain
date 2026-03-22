# Design System Specification: The Architectural Ledger (Equity Ledger)

## 1. Overview & Creative North Star
**Creative North Star: The Architectural Ledger**
In high-stakes financial reporting, trust is built through precision, clarity, and structural integrity. This design system, "The Architectural Ledger," treats data as a physical building block—heavy, stable, and perfectly aligned. It avoids "tech-startup" whimsy in favor of institutional-grade authority.

## 2. Visual Principles
*   **Precision over Decoration:** Every line and margin serves a functional purpose. 
*   **Data Density:** Information is presented clearly but compactly to allow for deep analysis without scrolling fatigue.
*   **Structural Contrast:** Uses clear borders and subtle tonal shifts to define hierarchy rather than heavy shadows or gradients.
*   **The "Paper to Pixel" Bridge:** Typography and layout are inspired by high-end financial broadsheets and technical ledgers.

## 3. Visual Language

### A. Color Palette
The system supports two primary modes, maintaining a core brand identity of **Deep Slate (#0F172A)** and **Emerald Accents**.

#### Light Mode (Standard)
*   **Surface Primary:** #FFFFFF (Pure White)
*   **Surface Secondary:** #F8FAFC (Slate 50) - Used for sidebars and background grounding.
*   **Text Primary:** #0F172A (Slate 900) - For headers and critical data.
*   **Text Secondary:** #475569 (Slate 600) - For supporting labels and metadata.
*   **Borders:** #E2E8F0 (Slate 200) - Fine 1px lines for structural definition.

#### Dark Mode (Obsidian)
*   **Surface Primary:** #0F172A (Deep Slate)
*   **Surface Secondary:** #1E293B (Slate 800) - For cards and sectioning.
*   **Text Primary:** #F1F5F9 (Slate 100)
*   **Text Secondary:** #94A3B8 (Slate 400)
*   **Borders:** #334155 (Slate 700)

#### Functional Colors (Shared)
*   **Positive (Success):** #10B981 (Emerald 500) - Used for gains and "Bullish" signals.
*   **Negative (Error):** #EF4444 (Red 500) - Used for losses and "Volatility" warnings.
*   **Neutral (Information):** #3B82F6 (Blue 500) - Used for primary CTAs and active states.

### B. Typography
*   **Primary Font:** **Manrope**
    *   **Scale:**
        *   *Display:* 32px / Bold (Total Equity)
        *   *Header 1:* 20px / Bold (Section Titles)
        *   *Body Large:* 16px / SemiBold (Table Data)
        *   *Body Small:* 12px / Medium (Labels/Captions)
*   **Spacing & Line Height:** Tight line-heights (1.2 - 1.4) to maintain data density.

### C. Shape & Form
*   **Roundness:** **4px (Small)**. Just enough to feel modern, but sharp enough to feel professional.
*   **Shadows:** Minimal to none. Elevation is communicated through border depth and color value shifts.

## 4. Key Components

### A. The Data Ledger (Table)
*   **Header:** Sticky with #F8FAFC background. 12px uppercase labels with 0.05em tracking.
*   **Rows:** 48px height. Subtle hover state (#F1F5F9). 
*   **Visual Indicators:** Small pill-style tags for Risk Scores (High/Low/Medium).

### B. The Navigation Rail
*   **Width:** 256px (Standard)
*   **Hierarchy:** Clear separation between primary nav (Portfolio, Ledger) and utility nav (Support, Logout).

### C. Insight Cards
*   Flat design with 1px border. 
*   Includes a "Title + Subtext" header for context before presenting raw numbers.

## 5. Implementation Notes
*   **Responsive Strategy:** Desktop-first. Sidebars collapse to icons on smaller viewports.
*   **Accessibility:** Ensure all data text maintains a contrast ratio of at least 4.5:1 against its background.
