# System Specification

## 1. Overview

This document specifies the intended behavior of the portfolio decision engine that consumes `opx` market data and option analytics.

The engine is not the data-collection layer itself. `opx` provides the normalized dataset, screening metrics, freshness signals, and ranking inputs. The system defined here is the deterministic decision layer that uses that data to recommend portfolio actions.

## 2. Core Objective

Maximize consistent weekly income from options premium while maintaining strict risk control and portfolio discipline.

The system is intended to:

- generate theta-driven returns through disciplined premium capture
- preserve conservative portfolio structure rather than chase isolated high-yield contracts
- limit directional, assignment, and concentration risk
- make repeatable, auditable decisions through explicit rules rather than ad hoc discretion

The system is not intended to:

- maximize raw premium without regard to risk
- act as a discretionary directional trading model
- override portfolio constraints for isolated high-income opportunities

## 3. High-Level Mechanics

For every existing options position, the engine evaluates the current state of the position and determines the best action:

- `Hold`
- `Roll`
- `Close`
- `Assign` in rare edge cases where assignment is explicitly allowed or structurally preferred

The engine applies decisions through a strict rule hierarchy so that risk controls take precedence over income optimization.

## 4. Position Evaluation

For each position, the engine first computes premium capture:

`Premium Captured % = (Premium collected - Cost to close) / Premium collected`

This metric answers how much of the original premium is already realized versus still exposed.

Interpretation:

- high premium capture means most of the income objective has already been achieved
- low premium capture late in the lifecycle means the remaining risk is often no longer worth the residual premium
- premium capture is a primary timing input for both profit-taking closes and standard rolls

## 5. Decision Hierarchy

Rules are applied in priority order. Lower-priority logic is evaluated only if higher-priority rules do not trigger.

### 5.1 Risk Override: Immediate Close

This is the highest-priority decision path.

Close immediately if the position violates core risk thresholds, including cases such as:

- excessively high absolute delta
- dangerous near-expiry gamma exposure
- very low remaining time with poor premium capture
- inconsistent or structurally invalid risk signals

Intent:

- eliminate positions where downside risk has overtaken remaining income value
- ensure risk always overrides premium-seeking behavior

### 5.2 Profit-Taking Close

Close when most of the original premium has already been captured.

Typical trigger:

- `premium_captured_pct >= 90%`

Intent:

- remove residual tail risk once the original income objective is substantially complete
- recycle capital instead of overstaying small remaining premium

### 5.3 Standard Roll

Roll when the current position is nearing expiration and has already captured a meaningful portion of its premium, provided the replacement contract improves or preserves position quality.

A standard roll requires:

- the current position to be within the roll timing window
- acceptable premium capture
- a replacement contract that produces a net credit
- equal or better theta than the current position
- equal or lower absolute delta than the current position

Intent:

- extend income generation while rolling up in quality rather than merely pushing risk forward

### 5.4 Forced Income Roll

This is a more aggressive roll path that can trigger outside the normal roll window when a substantially better income candidate exists.

A forced income roll requires:

- a net credit
- risk and mode compliance
- candidate theta at least `+5%` greater than the current position

Intent:

- allow the system to upgrade income efficiency when the quality improvement is material
- preserve discipline by still requiring credit and risk compliance

### 5.5 Default Hold

If none of the above rules trigger, the system holds the position.

Intent:

- avoid unnecessary turnover
- keep existing positions when they remain consistent with both income and risk objectives

## 6. Risk Modes

Positions are evaluated against explicit mode ranges.

### 6.1 Income Mode

Target absolute delta:

- `0.20 - 0.40`

Purpose:

- balanced premium collection with controlled assignment and directional risk

### 6.2 Exit Mode

Target absolute delta:

- `0.40 - 0.60`

Purpose:

- controlled use of higher-delta strikes when the system is intentionally steering toward an exit outcome

### 6.3 Accumulation Mode

Target absolute delta:

- typically `0.25 - 0.40`

Purpose:

- cautious put-side positioning when the portfolio is willing to accumulate exposure deliberately

Constraint:

- this mode is used sparingly and remains subordinate to portfolio-level controls

## 7. Hard Candidate Filters

Before a candidate can be considered for a roll, it must pass minimum quality and structural requirements.

Hard filters include:

- minimum premium threshold
- maximum acceptable bid-ask spread percentage
- minimum daily volume
- minimum open interest
- no extremely short-dated high-delta contracts
- no far-dated contracts

Intent:

- block poor executions before ranking begins
- keep the optimizer focused on realistic, tradable contracts

## 8. Candidate Selection and Ranking

When a roll is warranted, the engine scans eligible contracts in the same underlying and option side.

Only candidates that satisfy all hard filters, mode constraints, and net-credit requirements proceed to ranking.

Primary ranking order:

1. `iv_adjusted_premium_per_day`
2. `theta_efficiency`
3. `final_score`
4. `spread_score`

Typical behavior:

- the engine narrows the field to the best few candidates
- selection is usually made from the top three ranked contracts after all vetoes are applied

Intent:

- prioritize clean daily income generation
- favor candidates that combine strong decay, strong execution quality, and acceptable risk structure

## 9. Portfolio-Level Constraints

The engine enforces hard portfolio rules in addition to position-level logic.

These include:

- all trades must be net credit
- preserve a ladder of multiple expiration dates
- control aggregate assignment risk
- prevent unwanted increases in underlying share count
- avoid over-concentration in a single expiration date

Intent:

- keep the optimizer from improving one position at the cost of degrading the overall portfolio

## 10. Output Contract

The engine produces three primary output sections.

### 10.1 Positions

One record per current holding with:

- recommended action
- premium captured
- delta
- risk level
- supporting metrics used in the decision

### 10.2 Orders

Trade instructions for positions that should change, including:

- buy-to-close / sell-to-open legs
- net credit per contract
- total net credit
- suggested limit price

### 10.3 Summary

Portfolio-level impact summary including:

- projected premium impact
- projected theta impact
- risk distribution across delta bands
- structural effects on expiration distribution

## 11. Design Principles

The system is governed by the following design principles:

- theta-focused income generation with conservative delta management
- preference for rolling up in quality instead of merely extending duration
- deterministic priority ordering so risk always outranks income
- full auditability through explicit rules and measurable thresholds

## 12. Metric Framework

The engine does not rely on one signal. It evaluates a layered set of objective metrics that together determine whether a position should be held, closed, rolled, or assigned.

These metrics are grouped into six categories.

### 12.1 Premium Capture Metrics

#### Premium Captured %

Formula:

`(Premium collected - Cost to close) / Premium collected`

Role:

- primary realized-profit gauge
- profit-taking trigger
- roll-timing enabler

Typical interpretations:

- `>= 90%` triggers immediate profit-taking close
- `>= 70%` with approaching expiration enables standard roll logic
- `< 40%` with `DTE <= 7` contributes to risk-override close conditions

### 12.2 Time-Based Metrics

#### Days to Expiration (DTE)

Role:

- controls timing windows
- manages gamma exposure
- shapes the expiration ladder

Typical interpretations:

- `DTE <= 14` plus sufficient premium capture opens the standard roll window
- `DTE <= 7` with poor premium capture can force a close
- `DTE <= 3` with `abs(delta) > 0.35` blocks new candidates
- `DTE > 45` blocks new candidates

Portfolio intent:

- maintain a distributed ladder of expirations rather than bunching risk into one date

### 12.3 Risk and Exposure Metrics

#### Absolute Delta

This is the system's primary risk dial.

Role:

- drives mode checks
- drives risk overrides
- constrains roll eligibility

Typical interpretations:

- Income Mode: `0.20 - 0.40`
- Exit Mode: `0.40 - 0.60`
- Accumulation Mode: `0.25 - 0.40`
- `> 0.60` triggers immediate risk-override close

Roll requirement:

- standard rolls require `abs(new_delta) <= abs(current_delta)`
- all candidates must remain inside the relevant mode range

#### Risk Level and Risk Model Inconsistent Flag

Role:

- captures pre-computed risk severity
- flags disagreement between model inputs

Interpretation:

- any inconsistency flag triggers immediate close
- high risk level lowers ranking priority and can veto candidates entirely

### 12.4 Income and Efficiency Metrics

#### Theta Dollars per Day

Role:

- measures current and candidate daily decay generation

Typical rules:

- standard roll requires `candidate_theta >= current_theta`
- forced income roll requires `candidate_theta >= 1.05 * current_theta`

#### IV-Adjusted Premium per Day

Role:

- primary ranking metric for eligible candidates
- measures daily income adjusted for implied-volatility context

#### Theta Efficiency

Role:

- secondary ranking metric
- measures how efficiently decay is being generated relative to risk or capital usage

### 12.5 Liquidity and Execution Quality Metrics

#### Bid-Ask Spread % of Mid

Role:

- primary execution-friction filter

Rule:

- must be `<= 25%`

#### Volume and Open Interest

Role:

- ensure sufficient trading activity and contract depth

Rules:

- `volume >= 10`
- `open_interest >= 100`

#### Bid Price

Role:

- prevents the system from pursuing trivial premium when enabled

Rule:

- `bid >= filters_min_bid` when `filters_min_bid` is set; disabled by default (previously `0.50`)

### 12.6 Composite Quality Metrics

#### Final Option Score

Role:

- aggregated ranking input built from the normalized scoring framework

#### Spread Score

Role:

- fourth tie-breaker in candidate ranking
- rewards cleaner execution conditions

#### Passes Primary Screen

Role:

- binary gate summarizing whether the contract clears all core hard filters

Requirement:

- must be `true` before deeper evaluation proceeds

## 13. Decision Interaction Model

The metrics above are not combined arbitrarily. They are applied in strict order:

1. Risk overrides
2. Profit-taking close rules
3. Roll eligibility checks
4. Candidate ranking among surviving contracts
5. Portfolio-level vetoes

This ordering ensures:

- risk dominates income
- liquidity and execution quality dominate cosmetic ranking improvements
- portfolio structure dominates single-position optimization

## 14. System Character

The intended behavior of the engine is:

- theta-maximizing but conservative
- deterministic and repeatable
- suitable for audit, review, and stress testing
- dependent on a layered set of measurable signals rather than a single threshold or score

The resulting system should produce disciplined, explainable actions that can be inspected after the fact and tuned without changing the core decision philosophy.
