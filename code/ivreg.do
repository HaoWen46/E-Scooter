*******************************************************
* run_iv.do  — Clean IV pipeline (HDFE-friendly)
* Uses:
*   - IVs: subsidy + old_for_new policy terms (currently w1/w2/w3 as written)
*   - Controls: store "iv_*" vars (all 6 chains)
*******************************************************

clear all
version 16.0
cap log close
set more off
cap set matsize 10000

* --------- paths ----------
global INPUT_CSV   "../input/scooter_PBGN_o4n.csv"
global OUT_DIR     "../output/table/regtable"
cap mkdir "$OUT_DIR"

global MAIN_TEX    "$OUT_DIR/output.tex"
global FS_TEX      "$OUT_DIR/first_stage.tex"
global SUM_TEX     "$OUT_DIR/summary.tex"
global DEBUG_LOG   "$OUT_DIR/debug_trace.txt"

* --------- Start debug log ----------
cap log close _debug
log using "$DEBUG_LOG", text replace name(_debug)

* --------- packages ----------
cap which reghdfe
if _rc ssc install reghdfe, replace
cap which ivreghdfe
if _rc ssc install ivreghdfe, replace
cap which esttab
if _rc ssc install estout, replace


*******************************************************
* HELPER PROGRAM: add FE/demographics flags to estout
*******************************************************
capture program drop _add_fe_flags
program define _add_fe_flags
    syntax, DATEFE(integer) COUNTYFE(integer) DISTFE(integer) DEMO(integer)

    if `datefe' == 1  estadd local date_fe   "Yes"
    else              estadd local date_fe   "No"

    if `countyfe' == 1  estadd local county_fe "Yes"
    else                estadd local county_fe "No"

    if `distfe' == 1  estadd local dist_fe "Yes"
    else              estadd local dist_fe "No"

    if `demo' == 1  estadd local demo_fe "Yes"
    else            estadd local demo_fe "No"
end


*******************************************************
* 1) LOAD CSV + BUILD MONTHLY DATE
*******************************************************

import delimited using "$INPUT_CSV", clear stringcols(_all)

count
di as txt "DIAGNOSTIC [After import]: N = " r(N)

* Parse daily date to %td
cap gen date_d = date(date, "YMD")
if _rc cap gen date_d = date(date, "MDY")
format date_d %td
drop date
rename date_d date

count if missing(date)
if r(N) > 0 {
    di as err "FATAL: " r(N) " rows have invalid dates."
    exit 198
}

* Collapse to month (just convert daily to monthly index)
gen mdate = mofd(date)
format mdate %tm
drop date
rename mdate date

count
di as txt "DIAGNOSTIC [After Section 1 - date processing]: N = " r(N)


*******************************************************
* 2) COUNTY/DISTRICT NAMES + ID
*******************************************************

cap confirm string variable county
if _rc tostring county, replace force
cap confirm string variable district
if _rc tostring district, replace force

gen county_trad   = subinstr(county,   "台", "臺", .)
gen district_trad = subinstr(district, "台", "臺", .)
gen county_district = county_trad + "：" + district_trad

encode county,          gen(cty_id)
encode county_district, gen(cd_id)

count
di as txt "DIAGNOSTIC [After Section 2]: N = " r(N)


*******************************************************
* 3) BASIC FILTERS (if any)
*******************************************************

di as txt ""
di as txt "=== PRE-FILTER DIAGNOSTIC ==="
di as txt "Observations before filter: " _N
di as txt ""

* Example if you ever filter:
* keep if pleague=="PBGN"

count
di as txt "DIAGNOSTIC [After Section 3 filter]: N = " r(N)
if r(N) == 0 {
    di as err "FATAL: Filter removed all observations!"
    exit 198
}


*******************************************************
* 4) DESTRING NUMERIC VARIABLES
*******************************************************

local numeric_vars ///
    nstation ln_nstation ln_installed_base ///
    hh_size popdensity median_inc pct_female ///
    pct_between_20_29 pct_between_30_39 pct_between_40_49 ///
    pct_between_50_59 pct_above_60 pct_less_hs pct_above_college ///
    pct_executive pct_professional pct_technician pct_administrative ///
    pct_service pct_skilled pct_machinery pct_laborer ///
    ln_subsidy_w1_total ln_subsidy_w2_total ln_subsidy_w3_total ///
    ln_lag1y_subsidy_w1_total ln_lag1y_subsidy_w2_total ln_lag1y_subsidy_w3_total ///
    ln_old_for_new_w1_total ln_old_for_new_w2_total ln_old_for_new_w3_total ///
    ln_lag1y_old_for_new_w1_total ln_lag1y_old_for_new_w2_total ln_lag1y_old_for_new_w3_total ///
    iv_7eleven iv_familymart iv_okhilife iv_pxmart iv_carrefour iv_rtmartsimplemart

foreach v of local numeric_vars {
    cap confirm variable `v'
    if !_rc {
        cap destring `v', replace force
    }
}

count
di as txt "DIAGNOSTIC [After Section 4 - destring]: N = " r(N)

*** labeling

la var median_inc "Median income"
la var popdensity "Pop density"
la var ln_nstation "ln(all station)"

la var iv_7eleven "ln(\# 7-Eleven) $\times$ ln(lagged national stations)"
la var iv_familymart "ln(\# Family Mart) $\times$ ln(lagged national stations)"
la var iv_okhilife "ln(\# Other convenience stores) $\times$ ln(lagged national stations)"
la var iv_pxmart "ln(\# PX Mart) $\times$ ln(lagged national stations)"
la var iv_carrefour "ln(\# Carrefour) $\times$ ln(lagged national stations)"
la var iv_rtmartsimplemart "ln(\# Other grocery stores) $\times$ ln(lagged national stations)"


*******************************************************
* 5) CHECK CORE Y/E VARIABLES EXIST
*******************************************************

cap confirm variable ln_nstation
if _rc {
    di as err "FATAL: missing ln_nstation (dependent variable)"
    exit 198
}

cap confirm variable ln_installed_base
if _rc {
    di as err "FATAL: missing ln_installed_base (endogenous variable)"
    exit 198
}

count
di as txt "DIAGNOSTIC [After Section 5 - core vars check]: N = " r(N)


*******************************************************
* 6) DEFINE CONTROLS & IV LISTS
*******************************************************

* Demographic controls
local demo_controls ///
    hh_size popdensity median_inc pct_female ///
    pct_between_20_29 pct_between_30_39 pct_between_40_49 ///
    pct_between_50_59 pct_above_60 pct_less_hs pct_above_college ///
    pct_executive pct_professional pct_technician pct_administrative ///
    pct_service pct_skilled pct_machinery pct_laborer

* Policy instruments (as you currently wrote them)
local ivs_policy ///
    ln_subsidy_w2_total ln_subsidy_w3_total ///
    ln_lag1y_subsidy_w2_total ln_lag1y_subsidy_w3_total ///
    ln_old_for_new_w2_total ln_old_for_new_w3_total ///
    ln_lag1y_old_for_new_w2_total ln_lag1y_old_for_new_w3_total

* quick collinearity sanity check
pwcorr ln_subsidy_w1_total ln_subsidy_w2_total ln_subsidy_w3_total

* Store-level “iv_*” variables used as EXOGENOUS CONTROLS (not instruments)
local store_ctrls_raw ///
    iv_7eleven ///
    iv_familymart ///
    iv_okhilife ///
    iv_pxmart ///
    iv_carrefour ///
    iv_rtmartsimplemart

* Keep only IVs that actually exist
local ivs ""
foreach v of local ivs_policy {
    cap confirm variable `v'
    if !_rc local ivs `ivs' `v'
}

if "`ivs'"=="" {
    di as err "FATAL: No valid IV variables found (subsidy + old_for_new)."
    exit 198
}

* Keep only store controls that exist
local store_ctrls ""
foreach v of local store_ctrls_raw {
    cap confirm variable `v'
    if !_rc local store_ctrls `store_ctrls' `v'
}

di as txt "Using policy IVs: `ivs'"
di as txt "Using store controls: `store_ctrls'"


*******************************************************
* 7) REGRESSION SAMPLE — DROP MISSING
*******************************************************

count
di as txt "DIAGNOSTIC [Before dropping missing]: N = " r(N)

local dropcols ln_nstation ln_installed_base `ivs' `demo_controls' `store_ctrls_kept'
foreach c of local dropcols {
    cap confirm variable `c'
    if !_rc {
        count if missing(`c')
        local nmiss = r(N)
        if `nmiss' > 0 {
            di as txt "  Dropping " `nmiss' " obs with missing `c'"
            drop if missing(`c')
        }
    }
}

count
di as txt "DIAGNOSTIC [After Section 7 - final sample]: N = " r(N)
if r(N) == 0 {
    di as err "FATAL: All observations dropped due to missing values!"
    exit 198
}


*******************************************************
* 8) SUMMARY STATS (LaTeX)
*******************************************************

local sumcols ln_nstation ln_installed_base `ivs' `demo_controls'

estpost tabstat `sumcols', statistics(count mean sd min p25 p50 p75 max) columns(statistics)

esttab using "$SUM_TEX", ///
    cells("count(fmt(%9.0fc)) mean(fmt(%9.2f)) sd(fmt(%9.2f)) min(fmt(%9.2f)) p25(fmt(%9.2f)) p50(fmt(%9.2f)) p75(fmt(%9.2f)) max(fmt(%9.2f))") ///
    label title("Descriptive Statistics — PBGN Sample") ///
    nonumbers noobs booktabs nomtitles replace ///
    collabels("N" "Mean" "Std. Dev." "Min" "p25" "Median" "p75" "Max")


*******************************************************
* 9) RUN 5 SPECS: OLS + 2SLS + DIAGNOSTICS
*******************************************************

local store_ctrls_kept "`store_ctrls'"

est clear
local models_OLS ""
local models_2LS ""
local labels_OLS ""
local labels_2LS ""
local fe_models ""

forvalues s = 1/3 {

    local absorb ""
    local demo_on   = 1
    local county_on = 0
    local dist_on   = 0
    local date_on   = 0

    if `s'==2 {
        local absorb "`absorb' date"
        local date_on = 1
    }
    if `s'==3 {
        local absorb "`absorb' date"
        local absorb "`absorb' cty_id"
        local date_on = 1
        local county_on = 1
    }

    * Build control list
    local controls ""
    if `demo_on'==1 local controls "`controls' `demo_controls'"
    if "`store_ctrls_kept'" != "" local controls "`controls' `store_ctrls_kept'"

    di as txt "---- Spec `s' | absorb:`absorb' | demo=`demo_on' ----"

    ***************************************************
    * 9A) OLS
    ***************************************************
    if "`absorb'"=="" {
        reghdfe ln_nstation ln_installed_base `controls', ///
            vce(cluster cd_id) tol(1e-7)
    }
    else {
        reghdfe ln_nstation ln_installed_base `controls', ///
            absorb(`absorb') vce(cluster cd_id) tol(1e-7)
        local fe_models `fe_models' ols`s'
    }
    estimates store ols`s'
    quietly _add_fe_flags, datefe(`date_on') countyfe(`county_on') distfe(`dist_on') demo(`demo_on')
    local models_OLS `models_OLS' ols`s'
    local labels_OLS `"`labels_OLS' "OLS""'

    ***************************************************
    * 9B) 2SLS
    ***************************************************
    if "`absorb'"=="" {
        ivreghdfe ln_nstation (ln_installed_base = `ivs') `controls', ///
            vce(cluster cd_id) tol(1e-7)
    }
    else {
        ivreghdfe ln_nstation (ln_installed_base = `ivs') `controls', ///
            absorb(`absorb') vce(cluster cd_id) tol(1e-7)
        local fe_models `fe_models' iv`s'
    }
    cap estadd scalar j  = e(j)
    cap estadd scalar jp = e(jp)
    estimates store iv`s'
    quietly _add_fe_flags, datefe(`date_on') countyfe(`county_on') distfe(`dist_on') demo(`demo_on')
    local models_2LS `models_2LS' iv`s'
    local labels_2LS `"`labels_2LS' "2SLS""'

    ***************************************************
    * 9C) FIRST STAGE
    ***************************************************
    tempvar Ehat
    if "`absorb'"=="" {
        reghdfe ln_installed_base `ivs' `controls', ///
            vce(cluster cd_id) tol(1e-7)
    }
    else {
        reghdfe ln_installed_base `ivs' `controls', ///
            absorb(`absorb') vce(cluster cd_id) tol(1e-7)
    }
    cap testparm `ivs'
    if _rc==0 {
        estadd scalar fs  = r(F)
        estadd scalar fsp = r(p)
    }
    else {
        estadd scalar fs  = .
        estadd scalar fsp = .
    }
    predict double `Ehat' if e(sample), xb
    estimates store fs`s'
    quietly _add_fe_flags, datefe(`date_on') countyfe(`county_on') distfe(`dist_on') demo(`demo_on')

    * IV proxy adj R^2
    quietly {
        if "`absorb'"=="" {
            reghdfe ln_nstation `Ehat' `controls', ///
                vce(cluster cd_id) tol(1e-7)
        }
        else {
            reghdfe ln_nstation `Ehat' `controls', ///
                absorb(`absorb') vce(cluster cd_id) tol(1e-7)
        }
    }
    * scalar r2proxy = e(r2_a)
    est restore iv`s'
    * estadd scalar r2_a_iv_proxy = r2proxy
}


*******************************************************
* 10) MAIN TABLE (OLS + 2SLS)
*******************************************************

global FE_label date   "Year-Month FE" ///
                cty_id "County FE" ///
                cd_id  "District FE"

local fe_indicate ""
if "`fe_models'" != "" {
    estfe `fe_models', labels($FE_label)
    local fe_indicate `"`r(indicate_fe)'"'
}

* show main parameter + ALL store controls used
local keep_main ln_installed_base `store_ctrls_kept'

local models `models_OLS' `models_2LS'
local labels `labels_OLS' `labels_2LS'

esttab `models' using "$MAIN_TEX", ///
    keep(`keep_main') ///
    se(%6.3f) ///
    star(* 0.10 ** 0.05 *** 0.01) ///
    interaction(" $\times$ ") ///
    scalars("demo_fe Demo Controls" ///
            "date_fe Year-Month FE" ///
            "county_fe County FE" ///
            "N Observations" ///
            "j Sargan Stat" ///
            "jp Sargan p-value") ///
    sfmt(%~12s %~12s %~12s %9.0fc %4.3f %4.3f %4.2f %4.3f) ///
    mtitle(`labels') ///
    noconstant ///
    nonotes ///
    label ///
    booktabs ///
    replace ///
    title("OLS \& 2SLS Results — PBGN Sample (Dependent variable: \textit{nstation})")


*******************************************************
* 11) FIRST STAGE TABLE
*******************************************************

* use same store controls in FS table
local storeivs "`store_ctrls_kept'"
local keep_fs `ivs' `storeivs'

esttab fs1 fs2 fs3 using "$FS_TEX", ///
    keep(`keep_fs') ///
    se(%6.3f) ///
    star(* 0.10 ** 0.05 *** 0.01) ///
    interaction(" $\times$ ") ///
    scalars("demo_fe Demo Controls" ///
            "date_fe Year-Month FE" ///
            "county_fe County FE" ///
            "fs F-stat") ///
    sfmt(%~12s %~12s %~12s %~12s %9.0fc %4.3f %4.2f %4.3f) ///
    noconstant ///
    nonotes ///
    nodepvars ///
    nomtitles ///
    label ///
    booktabs ///
    replace ///
    title("IV First Stage — PBGN Sample")

di as txt ""
di as txt "======================================================"
di as txt "SUCCESS! LaTeX outputs created:"
di as txt "  Summary  -> $SUM_TEX"
di as txt "  Main     -> $MAIN_TEX"
di as txt "  1stStage -> $FS_TEX"
di as txt "======================================================"

* --------- Close debug log ----------
log close _debug
di as txt "  Debug    -> $DEBUG_LOG"

*******************************************************
