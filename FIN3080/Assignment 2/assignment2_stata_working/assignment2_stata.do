clear all
set more off
set scheme s1mono

local root = subinstr("`c(pwd)'", "\", "/", .)
local rawdir "`root'/raw"
local outdir "`root'/outputs"
local figdir "`outdir'/figures"
local tabdir "`outdir'/tables"

cap mkdir "`outdir'"
cap mkdir "`figdir'"
cap mkdir "`tabdir'"

tempfile trading_data index_data margin_data fund_monthly valuation_data price_data summary_data current_trend_data

* Trading value data
import delimited using "`rawdir'/market_trading_value.csv", clear varnames(1) encoding(utf8)
rename *, lower
keep if markettype == 21
gen date = date(trddt, "YMD")
format date %td
destring cnvaltrdtl, replace force
keep date cnvaltrdtl
duplicates drop date, force
sort date
save `trading_data', replace

* Index price data
clear
save `index_data', emptyok replace
forvalues i = 1/4 {
    import delimited using "`rawdir'/index_price_`i'.csv", clear varnames(1) encoding(utf8)
    rename *, lower
    gen date = date(idxtrd01, "YMD")
    format date %td
    destring idxtrd05, replace force
    keep date idxtrd05
    rename idxtrd05 index_close
    append using `index_data'
    save `index_data', replace
}
use `index_data', clear
bysort date: keep if _n == _N
sort date
save `index_data', replace

* Margin balance data
clear
save `margin_data', emptyok replace
forvalues i = 1/8 {
    import delimited using "`rawdir'/margin_balance_`i'.csv", clear varnames(1) encoding(utf8)
    rename *, lower
    keep mtdate mtbalance
    gen date = date(mtdate, "YMD")
    format date %td
    destring mtbalance, replace force
    collapse (sum) mtbalance, by(date)
    append using `margin_data'
    save `margin_data', replace
}
use `margin_data', clear
collapse (sum) mtbalance, by(date)
sort date
rename mtbalance margin_balance
save `margin_data', replace

* Fund monthly data
use "`rawdir'/daily_fund_shares.dta", clear
capture confirm string variable Date
if _rc == 0 {
    gen double date = daily(Date, "YMD")
}
else {
    gen double date = Date
}
format date %td
destring Scale, replace force
gen month = mofd(date)
format month %tm
collapse (sum) Scale, by(month)
rename Scale fund_shares
sort month
save `fund_monthly', replace

* Valuation and bond data
use "`rawdir'/csi300_pe_dividend.dta", clear
capture confirm string variable Date
if _rc == 0 {
    gen double date = daily(Date, "YMD")
}
else {
    gen double date = Date
}
format date %td
destring CSI300_PE, replace force
destring CSI300_DividendYield, replace force
keep date CSI300_PE CSI300_DividendYield
sort date
save `valuation_data', replace

import delimited using "`rawdir'/bond_10y_yield.csv", clear varnames(1) encoding(utf8)
rename *, lower
gen date = date(trddt, "YMD")
format date %td
destring cvtype, replace force
destring yeartomatu, replace force
destring yield, replace force
keep if cvtype == 1 & yeartomatu == 10
keep date yield
rename yield bond_yield_pct
sort date
merge 1:1 date using `valuation_data', keep(match) nogen
gen erp = 1 / CSI300_PE - bond_yield_pct / 100
gen dividend_spread = (CSI300_DividendYield - bond_yield_pct) / 100
keep date CSI300_PE CSI300_DividendYield bond_yield_pct erp dividend_spread
sort date
save `valuation_data', replace

* Representative stock prices
use "`rawdir'/daily_representative_price.dta", clear
capture confirm string variable Date
if _rc == 0 {
    gen double date = daily(Date, "YMD")
}
else {
    gen double date = Date
}
format date %td
capture confirm numeric variable stk_code
if _rc == 0 {
    gen str6 code = string(stk_code, "%06.0f")
}
else {
    gen str6 code = substr("000000" + stk_code, strlen("000000" + stk_code) - 5, 6)
}
destring Price, replace force
keep date code Price
drop if missing(date) | missing(code) | missing(Price)
sort code date
save `price_data', replace

local brokers "000166 000686 000712 000728 000750 000776 000783 002500 002670 002673 002736 002797 002926 002939 002945 600030 600061 600095 600109 600155 600369 600621 600837 600864 600906 600909 600918 600958 600999 601059 601066 601099 601108 601136 601162 601198 601211 601236 601375 601377 601456 601555 601688 601696 601788 601878 601881 601901 601990 601995"

tempname post_summary
postfile `post_summary' str10 episode double(start_date end_date trading_value_ratio max_trading_value_ratio trading_peak_date sse_before_peak_return sse_after_peak_return margin_balance_ratio fund_issuance_ratio erp_start_pct erp_end_pct erp_start_percentile erp_end_percentile dividend_spread_start_pct dividend_spread_end_pct dividend_spread_start_percentile dividend_spread_end_percentile eastmoney_return hundsun_return best_broker_return) str6 best_broker_code using `summary_data', replace

foreach ep in 2015 2021 current {
    if "`ep'" == "2015" {
        local start = td(19jun2014)
        local end = td(12jun2015)
    }
    if "`ep'" == "2021" {
        local start = td(04jan2019)
        local end = td(18feb2021)
    }
    if "`ep'" == "current" {
        local start = td(18sep2024)
        local end = td(09mar2026)
    }

    * Trading value and peak
    use `trading_data', clear
    quietly summarize cnvaltrdtl if date == `start', meanonly
    scalar trading_start = r(mean)
    quietly summarize cnvaltrdtl if date == `end', meanonly
    scalar trading_end = r(mean)
    keep if inrange(date, `start', `end')
    gsort -cnvaltrdtl date
    scalar trading_peak_value = cnvaltrdtl[1]
    scalar trading_peak_date = date[1]

    * Index returns before and after peak
    use `index_data', clear
    quietly summarize index_close if date == `start', meanonly
    scalar index_start = r(mean)
    quietly summarize index_close if date == `end', meanonly
    scalar index_end = r(mean)
    quietly summarize index_close if date == trading_peak_date, meanonly
    scalar index_peak = r(mean)

    * Margin balance
    use `margin_data', clear
    quietly summarize margin_balance if date == `start', meanonly
    scalar margin_start = r(mean)
    quietly summarize margin_balance if date == `end', meanonly
    scalar margin_end = r(mean)

    * Fund issuance ratio
    local start_month = mofd(`start')
    if "`ep'" == "current" {
        local end_month = ym(2026, 2)
    }
    else {
        local end_month = mofd(`end')
    }
    use `fund_monthly', clear
    keep if inrange(month, `start_month', `end_month')
    gen dist_start = abs(month - `start_month')
    sort dist_start month
    scalar fund_start = fund_shares[1]
    gen dist_end = abs(month - `end_month')
    sort dist_end month
    scalar fund_end = fund_shares[1]

    * ERP and dividend spread
    use `valuation_data', clear
    quietly summarize erp if date == `start', meanonly
    scalar erp_start = r(mean)
    quietly summarize erp if date == `end', meanonly
    scalar erp_end = r(mean)
    quietly summarize dividend_spread if date == `start', meanonly
    scalar spread_start = r(mean)
    quietly summarize dividend_spread if date == `end', meanonly
    scalar spread_end = r(mean)

    local hist_end = mdy(12, 31, year(`start') - 1)
    count if date <= `hist_end'
    scalar hist_n = r(N)
    count if date <= `hist_end' & erp <= erp_start
    scalar erp_start_pctile = 100 * r(N) / hist_n
    count if date <= `hist_end' & erp <= erp_end
    scalar erp_end_pctile = 100 * r(N) / hist_n
    count if date <= `hist_end' & dividend_spread <= spread_start
    scalar spread_start_pctile = 100 * r(N) / hist_n
    count if date <= `hist_end' & dividend_spread <= spread_end
    scalar spread_end_pctile = 100 * r(N) / hist_n

    * Representative stocks
    use `price_data', clear
    keep if inrange(date, `start', `end')
    preserve
        keep if code == "300059"
        sort date
        scalar eastmoney_return = Price[_N] / Price[1] - 1
    restore
    preserve
        keep if code == "600570"
        sort date
        scalar hundsun_return = Price[_N] / Price[1] - 1
    restore

    scalar best_broker_return = -100
    local best_broker_code ""
    foreach code of local brokers {
        preserve
            keep if code == "`code'"
            count
            if r(N) >= 2 {
                sort date
                scalar temp_return = Price[_N] / Price[1] - 1
                if temp_return > best_broker_return {
                    scalar best_broker_return = temp_return
                    local best_broker_code "`code'"
                }
            }
        restore
    }

    post `post_summary' ("`ep'") (`start') (`end') ///
        (trading_end / trading_start) ///
        (trading_peak_value / trading_start) ///
        (trading_peak_date) ///
        (index_peak / index_start - 1) ///
        (index_end / index_peak - 1) ///
        (margin_end / margin_start) ///
        (fund_end / fund_start) ///
        (erp_start * 100) ///
        (erp_end * 100) ///
        (erp_start_pctile) ///
        (erp_end_pctile) ///
        (spread_start * 100) ///
        (spread_end * 100) ///
        (spread_start_pctile) ///
        (spread_end_pctile) ///
        (eastmoney_return) ///
        (hundsun_return) ///
        (best_broker_return) ///
        ("`best_broker_code'")
}

postclose `post_summary'
use `summary_data', clear
format start_date end_date trading_peak_date %td
sort episode
save `summary_data', replace
export delimited using "`tabdir'/episode_summary_stata.csv", replace

* Identify current best brokerage code for trend graph
quietly levelsof best_broker_code if episode == "current", local(current_best_broker) clean

* Build current trend series
use `trading_data', clear
keep if inrange(date, td(18sep2024), td(09mar2026))
sort date
quietly summarize cnvaltrdtl if date == td(18sep2024), meanonly
scalar current_trading_start = r(mean)
gen trading_value_ratio = cnvaltrdtl / current_trading_start
gen max_trading_value_ratio = trading_value_ratio
replace max_trading_value_ratio = max(max_trading_value_ratio, max_trading_value_ratio[_n-1]) if _n > 1
keep date trading_value_ratio max_trading_value_ratio
save `current_trend_data', replace

use `margin_data', clear
keep if inrange(date, td(18sep2024), td(09mar2026))
quietly summarize margin_balance if date == td(18sep2024), meanonly
scalar current_margin_start = r(mean)
gen margin_balance_ratio = margin_balance / current_margin_start
keep date margin_balance_ratio
sort date
merge 1:1 date using `current_trend_data', nogen
save `current_trend_data', replace

use `valuation_data', clear
keep if inrange(date, td(18sep2024), td(09mar2026))
gen erp_pct = erp * 100
gen dividend_spread_pct = dividend_spread * 100
keep date erp_pct dividend_spread_pct
sort date
merge 1:1 date using `current_trend_data', keep(match using) nogen
save `current_trend_data', replace

use `fund_monthly', clear
quietly summarize fund_shares if month == ym(2024, 9), meanonly
scalar current_fund_start = r(mean)
keep month fund_shares
rename fund_shares fund_end_value
sort month
tempfile current_fund_monthly
save `current_fund_monthly', replace

use `current_trend_data', clear
gen month = mofd(date)
replace month = ym(2026, 2) if month > ym(2026, 2)
merge m:1 month using `current_fund_monthly', keep(master match) nogen
gen fund_issuance_ratio = fund_end_value / current_fund_start
drop month fund_end_value
save `current_trend_data', replace

use `price_data', clear
keep if code == "300059" & inrange(date, td(18sep2024), td(09mar2026))
sort date
scalar eastmoney_start = Price[1]
gen eastmoney_return = Price / eastmoney_start - 1
keep date eastmoney_return
sort date
tempfile eastmoney_data
save `eastmoney_data', replace

use `current_trend_data', clear
merge 1:1 date using `eastmoney_data', keep(master match) nogen
save `current_trend_data', replace

use `price_data', clear
keep if code == "600570" & inrange(date, td(18sep2024), td(09mar2026))
sort date
scalar hundsun_start = Price[1]
gen hundsun_return = Price / hundsun_start - 1
keep date hundsun_return
sort date
tempfile hundsun_data
save `hundsun_data', replace

use `current_trend_data', clear
merge 1:1 date using `hundsun_data', keep(master match) nogen
save `current_trend_data', replace

use `price_data', clear
keep if code == "`current_best_broker'" & inrange(date, td(18sep2024), td(09mar2026))
sort date
scalar broker_start = Price[1]
gen best_broker_return = Price / broker_start - 1
keep date best_broker_return
sort date
tempfile broker_data
save `broker_data', replace

use `current_trend_data', clear
merge 1:1 date using `broker_data', keep(master match) nogen
order date trading_value_ratio max_trading_value_ratio margin_balance_ratio fund_issuance_ratio erp_pct dividend_spread_pct eastmoney_return hundsun_return best_broker_return
sort date
save `current_trend_data', replace
export delimited using "`tabdir'/current_episode_trend_stata.csv", replace

* Graphs
use `current_trend_data', clear
format date %td

local xlabs `=td(01oct2024)' `=td(01apr2025)' `=td(01oct2025)' `=td(01mar2026)'

local gopts xlabel(`xlabs', format(%tdMonCCYY) angle(45) labsize(vsmall)) ///
    xtitle("") ///
    graphregion(color(white) margin(10 18 10 12)) ///
    plotregion(margin(8 12 6 8))

twoway line trading_value_ratio date, title("Trading Value Ratio", size(medsmall)) legend(off) name(g1, replace) `gopts'
twoway line max_trading_value_ratio date, title("Max Trading Value Ratio", size(medsmall)) legend(off) name(g2, replace) `gopts'
twoway line margin_balance_ratio date, title("Margin Balance Ratio", size(medsmall)) legend(off) name(g3, replace) `gopts'
twoway line fund_issuance_ratio date, title("Fund Issuance Ratio", size(medsmall)) legend(off) name(g4, replace) `gopts'
graph combine g1 g2 g3 g4, cols(2) imargin(2 2 2 2) graphregion(color(white) margin(6 10 6 6)) name(gmarket, replace)
graph export "`figdir'/current_trend_market_indicators.png", replace width(2000)

twoway line erp_pct date, title("CSI 300 ERP (%)", size(medsmall)) legend(off) name(g5, replace) `gopts'
twoway line dividend_spread_pct date, title("Dividend Yield - 10Y Yield (%)", size(medsmall)) legend(off) name(g6, replace) `gopts'
twoway line eastmoney_return date, title("East Money Return", size(medsmall)) legend(off) name(g7, replace) `gopts'
twoway line hundsun_return date, title("Hundsun Return", size(medsmall)) legend(off) name(g8, replace) `gopts'
graph combine g5 g6 g7 g8, cols(2) imargin(2 2 2 2) graphregion(color(white) margin(6 10 6 6)) name(gvaluation, replace)
graph export "`figdir'/current_trend_valuation_and_stocks.png", replace width(2000)

twoway line eastmoney_return date || line hundsun_return date || line best_broker_return date, ///
    title("Representative Stock Returns", size(medium)) ///
    legend(order(1 "East Money" 2 "Hundsun" 3 "Best Broker") rows(1) size(small)) ///
    `gopts' ///
    name(gstocks, replace)
graph export "`figdir'/current_trend_stock_comparison.png", replace width(2200)

graph drop _all

log using "`tabdir'/stata_run.log", replace text
use `summary_data', clear
list, abbrev(20)
log close

display "Stata pipeline finished successfully."
