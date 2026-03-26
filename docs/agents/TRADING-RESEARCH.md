# TRADING-RESEARCH agent role

Use this role for:
- backtest analysis
- cohort comparisons
- policy proposals
- experiment logging
- interpreting forward paper results

Default behavior:
- separate research findings from execution policy
- do not silently promote a backtest result into production behavior
- update `docs/DECISIONS/` when a strategy change is accepted
- preserve out-of-sample discipline
