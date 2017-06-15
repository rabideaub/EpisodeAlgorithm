#delimit;
set more off;
clear;
capture: log close;

/*******************************************************************************************************
 The purpose of this program is to read in the MDS2.0 and MDS3.0 and collapse the two down from the
 assessment level to the stay level.
*******************************************************************************************************/

global mds "/schaeffer-b/sch-protected/VERTICAL-INTEGRATION/Data/2009-13_PAC/Raw/MDS";
global temp "/schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Data/MDS";
global out "/schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Data/MDS";
global logdir "/schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Programs/MainAnalysis";

/*******************************************************************************************************
 READ IN THE MDS2 AND STANDARDIZE VARIABLES
*******************************************************************************************************/
use "${mds}/mds2006.dta",clear;
append using "${mds}/mds2007.dta" "${mds}/mds2008.dta" "${mds}/mds2009.dta" "${mds}/mds2010.dta";
describe, simple;
keep R4_DISCHARGE_DT AB1_ENTRY_DT A4A_REENTRY_DT A3A_ASMT_REF_DT BENE_ID TARGET_DATE AA8A_PRI_RFA AA6B_FAC_MCARE_NBR G1AA_SELF_BED G1BA_SELF_TRANS G1HA_SELF_EAT G1IA_SELF_TOLIET; 
count;

format R4_DISCHARGE_DT %td;
gen discharge_year=year(R4_DISCHARGE_DT);
tab discharge_year;

/*Reason for the Assessment*/
gen assessment_reason=AA8A_PRI_RFA;
gen admission_assess=1 if assessment_reason=="1" | assessment_reason=="01";
gen reentry=1 if assessment_reason=="9" | assessment_reason=="09";
/*1=admission
  2=annual
  3=change in status
  4=correction to prior full assessment
  5=quarterly
  6=discharge: return not anticipated
  7=discharge: return anticipated
  8=discharge prior to initial assessment
  9=reentry
  10=correction to quarterly assessment
  0=none of the abvove*/

gen version="MDS2";
rename A4A_REENTRY_DT reentry_dt;
gen entry_dt=max(reentry_dt,AB1_ENTRY_DT); /*keep the most recent one - sometimes reentries retain the original entry dates which is problematic*/
rename A3A_ASMT_REF_DT assessment_dt;
rename TARGET_DATE target_dt;
rename R4_DISCHARGE_DT disch_dt;
rename AA6B_FAC_MCARE_NBR provid;
rename BENE_ID bene_id;
rename G1AA_SELF_BED adl_bed_mobility;
rename G1BA_SELF_TRANS adl_transfer;
rename G1HA_SELF_EAT adl_eating;
rename G1IA_SELF_TOLIET adl_toiletry;
save "${temp}/mds2_temp.dta",replace;


/*******************************************************************************************************
 READ IN THE MDS3 AND STANDARDIZE VARIABLES
*******************************************************************************************************/
use "${mds}/mds_asmt_summary_3_2010.dta",clear;
append using "${mds}/mds_asmt_summary_3_2011.dta" "${mds}/mds_asmt_summary_3_2012.dta" "${mds}/mds_asmt_summary_3_2013.dta" "${mds}/mds_asmt_summary_3_2014.dta";
describe, simple;
keep BENE_ID A2000_DSCHRG_DT A1600_ENTRY_DT TRGT_DT A0310A_FED_OBRA_CD A1700_ENTRY_TYPE_CD A0100B_CMS_CRTFCTN_NUM G0110A1_BED_MBLTY_SE G0110B1_TRNSFR_SELF_ G0110H1_EATG_SELF_CD G0110I1_TOILTG_SELF_;
count;

gen DSCHRG_DT=date(A2000_DSCHRG_DT,"YMD");
gen discharge_year=year(DSCHRG_DT);
tab discharge_year;
gen version="MDS3";
rename DSCHRG_DT disch_dt;
rename A1600_ENTRY_DT entry_dt;
rename TRGT_DT target_dt;
rename BENE_ID bene_id;
rename A0100B_CMS_CRTFCTN_NUM provid;
rename G0110A1_BED_MBLTY_SE adl_bed_mobility;
rename G0110B1_TRNSFR_SELF_ adl_transfer;
rename G0110H1_EATG_SELF_CD adl_eating;
rename G0110I1_TOILTG_SELF_ adl_toiletry;

/*Reason for the Assessment*/
gen reentry=1 if A1700_ENTRY_TYPE_CD=="2";
gen assessment_reason=A0310A_FED_OBRA_CD;
gen admission_assess=1 if assessment_reason=="01";
/*01=admission
  02=quarterly
  03=annual
  04=correction to prior status assessment
  05=correction to prior full assessment
  06=correction to prior quarterly assessment
  99=none of the abvove*/
  

/*******************************************************************************************************
 COMBINE THE DATASETS AND GROUP ASSESSMENTS INTO STAYS
*******************************************************************************************************/
append using "${temp}/mds2_temp.dta";
count;
tab discharge_year;

/*The following logic is that everything between an explicit admission and explicit discharge is a single continuous stay.
  In the event that no admission or no discharge date is found, the default is to set admit_dt to
  the date of the first observed assessment (target_dt), and set disch_dt to the last assessment date +92 or the end of
  the observation period, whichever is sooner*/

/*From the first observation of a bene to the first discharge is a case. increment case after a discharge*/
gsort bene_id target_dt -disch_dt, mfirst;
gen case=1 if bene_id!=bene_id[_n-1]; 
gen case_increment=0;
replace case_increment=1 if disch_dt!=.;
replace case=case[_n-1] + case_increment[_n-1] if bene_id==bene_id[_n-1];
tab case, missing;

/*if an entry date exists on an intermediate observation in a case, set the first observation's sort_date to that entry date*/
sort bene_id case;

/*By design there should only be 1 discharge date per case. Populate all observations within a case with this discharge date*/
by bene_id case: egen max_disch_dt=max(disch_dt);
replace disch_dt=max_disch_dt;

/*The entry dates are much more precise in the MDS3, so if a case has an MDS3 observation, impute that entry date for the case*/
by bene_id case: egen admit_dt_v3=max(entry_dt) if version=="MDS3"; 

/*Take the most recent entry date in a case that is before the discharge date. We keep the most recent because in some instances an entry date is from an admission years before in used instead of the actual entry date*/ 
by bene_id case: egen max_admit_dt=max(entry_dt) if entry_dt<=max_disch_dt;


/*Take the last date actually written on a claim within a case (target date). This is the discharge date unless it was imputed, in which case it is the last assessment date*/
by bene_id case: egen last_dt=max(target_dt);
*by bene_id case: egen imputed_disch=max(imputed_discharge); 
replace disch_dt=min(last_dt+92,mdy(01,01,2015)) if disch_dt==.; /*If no discharge and no subsequent assessments, set discharge to the last assessment + 92, or the end of the observation window*/

gen admit_dt=admit_dt_v3 if admit_dt_v3!=. & admit_dt_v3<=disch_dt; /*If an admission date from MDS3 exists and is logical, use it*/
replace admit_dt=max_admit_dt if admit_dt_v3==. & max_admit_dt<=disch_dt; /*If no MDS3 admission date exists, use our best guess from the MDS2*/

/*Create variables for the first and last occurrences of select ADLs within a case*/
#delimit;
set more off;

sort bene_id case target_dt;
by bene_id case: gen assess_num=_n;

foreach adl in adl_bed_mobility adl_transfer adl_eating adl_toiletry {;
	/*Identify the first and observation in a case with a non-missing ADL score*/
	replace `adl'="" if `adl'=="*" | `adl'=="-";
	by bene_id case: egen first`adl'_num = min(cond(`adl' != "", assess_num, .));
	by bene_id case: egen last`adl'_num = max(cond(`adl' != "", assess_num, .));
	replace last`adl'_num=. if first`adl'_num==last`adl'_num; /*If only 1 ADL score exists, don't duplicate the first and last scores*/
	
	/*Populate all rows in a case with the first and last ADL and the date on which the assessment occurred*/
	gen f_`adl'=`adl' if first`adl'_num==assess_num;
	by bene_id case: egen first_`adl'=mode(f_`adl');
	by bene_id case: egen first_`adl'_dt=mode(target_dt) if first`adl'_num==assess_num;
	
	
	gen l_`adl'=`adl' if last`adl'_num==assess_num;
	by bene_id case: egen last_`adl'=mode(l_`adl');
	by bene_id case: egen last_`adl'_dt=mode(target_dt) if last`adl'_num==assess_num;
	
	drop l_`adl' f_`adl' first`adl'_num last`adl'_num;
};


#delimit;
set more off;

gen first_case=1 if case!=case[_n-1] | (bene_id!=bene_id[_n-1]); /*The first obs in a stay. Key vars are bene_id, provider_id, admit_dt, disch_dt, last_dt, and first_case*/
keep if first_case==1;

/*If still no admission date, use the target date on the first observation of the case if it indicates an admission, or 7 or 92 days before the target date otherwise*/
replace admit_dt=target_dt if admit_dt==. & assessment_reason=="1" | assessment_reason=="01";
replace admit_dt=target_dt-7 if admit_dt==. & assessment_reason=="8" | assessment_reason=="08"; /*If no initial assessment was completed, the stay likely started within a week of discharge*/
replace admit_dt=target_dt-92 if admit_dt==.;

save "${out}/mds_stays_2006_2014_1.dta",replace;

/*Do some diagnostics*/
gen disch_year=year(disch_dt);
gen admit_year=year(admit_dt);
gen los=disch_dt-admit_dt;

sort disch_year;
by disch_year: summarize los;
tab disch_year, missing;
tab admit_year, missing;
tab2 admit_year disch_year, missing;
