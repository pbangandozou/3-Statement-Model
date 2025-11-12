# 3-Statement-Model
Three-Statement Model (SEC XBRL → Excel & PDF)

A fast, CLI-driven Python tool that pulls SEC CompanyFacts (XBRL), builds historical actuals (2020–2024), infers assumptions, projects 2025–2029, links the Income Statement, Balance Sheet, and Cash Flow, and exports a formatted Excel workbook plus an optional analysis PDF.

What It Does

Resolve company from ticker or name using SEC mapping.

Fetch facts from SEC XBRL APIs with retry/backoff.

Build actuals (’20–’24) for IS/BS/CF with tag fallbacks.

Infer assumptions (growth, margins, CapEx, WC, taxes, interest).

Project forward (’25–’29) including a simple debt schedule.

Link statements into a coherent 3-statement model.

Export Excel with:

Income, Balance, Cash Flow (clean headers, formatting)

Assumptions sheet (editable)

Debt Schedule sheet

Export PDF summary (if reportlab is installed).

Key Features
Feature	Benefit
SEC XBRL ingestion	Public, programmatic fundamentals
Auto-assumption inference	Reasonable starting point from history
Debt schedule	Issuance/repay % of revenue, interest tie-out
Linked statements	Consistent IS/BS/CF across years
Polished Excel output	Ready to share and tweak
Optional PDF analysis	Narrative + KPI snapshot in one file
Outputs

Excel: {TICKER}_three_statement_model_2020_2029.xlsx

PDF (optional): {TICKER}_summary.pdf
