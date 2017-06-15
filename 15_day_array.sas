/*This program makes a dataset with variables of a bene's wherabouts for each day of an episode of care.
  It requires R.collapsed_&condition, which comes from the &condition_readmission_v2014_short.sas program*/


%let include = /schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Programs/MainAnalysis;
%include "&include./00_Assign_Macro_Variables_and_Libraries.sas";

%let year=&YY.&YYE.; /* e.g., 0910, or 11  */;
%let start_dt = "01jan04"d;
%let end_dt = "31dec13"d;
%let max_qtr=%sysfunc(sum(%sysfunc(intck(qtr,&start_dt., &end_dt.)),1));
%LET CONDITION=&DX.; 
%LET PATH1= /schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Data/MainAnalysis/Final; /*RAW DATA FILES PATH, MUST BE CHANGED */

%LET PATH4= /schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Data/MainAnalysis/Final; /* for derived data sets, MUST BE CHANGED */
%LET PATH5= /schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Programs/MainAnalysis;
			/* for SAS macros and HCC FORMAT CATALOG RESIDES, MUST BE CHANGED  */

*%let path6= /disk/agedisk3/medicare.work/goldman-DUA25731/rabideau/Programs/Readmission_Algorithm/CCMap2008_2012;  
*%let path7= /disk/agedisk3/medicare.work/goldman-DUA25731/rabideau/Programs/Readmission_Algorithm/CCS_2013;   

LIBNAME RAW "&PATH1"; 
LIBNAME R "&PATH4"; 

%include "&include./comoanaly2012_2015.sas";

%let out=/schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Output;

%let model_vars = CHF VALVE PULMCIRC PERIVASC HTN_C PARA NEURO CHRNLUNG DM
         		  DMCX HYPOTHY RENLFAIL LIVER ULCER AIDS LYMPH METS TUMOR ARTH COAG OBESE WGHTLOSS 
				  LYTES BLDLOSS ANEMDEF ALCOHOL DRUG PSYCH DEPRESS;

data day_array;
	set R.collapsed_&CONDITION._elix;
run;

%macro day_array(var);
	/*Determine the maximum number of stays in the episode of care for each facility type*/
	data sample_&condition. (keep=hicno case hicno_case case_count _factype age birth male rti_race_cd
								  ADMIT DISCH PROVID DIAG1 pmt_amt tot_chg DRGCD DDEST sslssnf
								  _ADMIT _DISCH _PROVID _DIAG1 _pmt_amt _tot_chg _DRGCD _DDEST _sslssnf);
		set R.sample_&CONDITION.;
		retain case_count;
		by hicno_case;
		if first.hicno_case then case_count=0;
		if upcase(_factype)="&var." then case_count+1;
	run;

	proc means data=sample_&condition. noprint missing;
	 var case_count;
	 output out=max_obs (drop=_FREQ_ _TYPE_)
	 max=max_case_count;
	run;

	data _null_;
	 set max_obs;
	 call symput ('N',Trim(Left(max_case_count)));
	run; 

	/*If any given day is between the admission and disch of a given factype, make the day variable = that factype*/
	data day_array (keep=hicno hicno_case PROVID ADMIT DISCH DEATH index_ccr pmt_amt tot_chg age birth male race rti_race_cd  
						 STA_admit: SNF_admit: IRF_admit: MDS_admit: LTC_admit: HHA_admit: 
						 STA_disch: SNF_disch: IRF_disch: MDS_disch: LTC_disch: HHA_disch:
						 STA_pmt_amt: SNF_pmt_amt: IRF_pmt_amt: MDS_pmt_amt: LTC_pmt_amt: HHA_pmt_amt:
						 STA_provid: SNF_provid: IRF_provid: MDS_provid: LTC_provid: HHA_provid:
					     STA_tot_chg: SNF_tot_chg: IRF_tot_chg: MDS_tot_chg: LTC_tot_chg: HHA_tot_chg:
						 STA_ccr: SNF_ccr: IRF_ccr: MDS_ccr: LTC_ccr: HHA_ccr: HHA_cpv:
						 STA_cost_allyr: SNF_cost_allyr: IRF_cost_allyr: MDS_cost_allyr: LTC_cost_allyr: HHA_cost_allyr:
						 cost_allyr radm30 radm45 dd30 year elix_cnt day:
						 CHF VALVE PULMCIRC PERIVASC HTN_C PARA NEURO CHRNLUNG DM
         		 		 DMCX HYPOTHY RENLFAIL LIVER ULCER AIDS LYMPH METS TUMOR ARTH COAG OBESE WGHTLOSS 
				  		 LYTES BLDLOSS ANEMDEF ALCOHOL DRUG PSYCH DEPRESS);
		length day1-day180 $4;
		set day_array;
			array day{180} $ day1-day180;
			do i=1 to dim(day);
			date=intnx('day',disch,i);
				%do j=1 %to &N.;
					if &var._admit&j.~=. & ((&var._admit&j.<=date<=&var._disch&j.) | (&var._admit&j.<=date & &var._disch&j.=.)) 
					then day[i]="&var.";
				%end;
				if DEATH~=. & DEATH<=date then day[i]="DEAD";
			end;

		format sta_admit1-sta_admit3 sta_disch1-sta_disch3 snf_admit1-snf_admit3 snf_disch1-snf_disch3
		   	   irf_admit1-irf_admit3 irf_disch1-irf_disch3 ltc_admit1-ltc_admit2 ltc_disch1-ltc_disch2
		   	   hha_admit1-hha_admit3 hha_disch1-hha_disch3 mds_admit1-mds_admit3 mds_disch1-mds_disch3/*out_admit1-out_admit3 out_disch1-out_disch3*/
		   	   DATE9.;
	run;
%mend;
%day_array(MDS); /*Order matters here because SNF will override MDS entries.*/
%day_array(HHA);
%day_array(LTC);
%day_array(IRF);
%day_array(SNF);
%day_array(STA);


proc sort data=day_array; by hicno_case; run;
/*proc print data=day_array (obs=10); run;*/

/*proc print data=day_array (obs=10);
	var day: MDS_admit1-MDS_admit3 MDS_disch1-MDS_disch3;
	where day1="MDS";
run;
*/


/*Make some flags*/
data freq_array;
	set day_array;
	array day {180} day1-day180;

	consec_com=0;
	com30=0;
	MDS=0;
	STA=0;
	SNF=0;
	LTC=0;
	IRF=0;
	HHA=0;
	COM=0;
	DEAD=0;

	tot_MDS=0;
	tot_STA=0;
	tot_SNF=0;
	tot_LTC=0;
	tot_IRF=0;
	tot_HHA=0;
	tot_COM=0;

	do i=1 to 180;
		if day[i]="MDS" then MDS=1;
		if day[i]="STA" then STA=1;
		if day[i]="SNF" then SNF=1;
		if day[i]="LTC" then LTC=1;
		if day[i]="IRF" then IRF=1;
		if day[i]="HHA" then HHA=1;
		if day[i]="DEAD" then DEAD=1;
		if day[i]="" then COM=1;

		if day[i]="MDS" then tot_MDS+1;
		if day[i]="STA" then tot_STA+1;
		if day[i]="SNF" then tot_SNF+1;
		if day[i]="LTC" then tot_LTC+1;
		if day[i]="IRF" then tot_IRF+1;
		if day[i]="HHA" then tot_HHA+1;
		if day[i]="" then tot_COM+1;

		/*See if a bene was ever in the community for 30consecutive days*/
		if day[i]="" then consec_com=consec_com+1;
		if day[i]~="" then consec_com=0;
		if consec_com>=30 then com30=1;
	end;
	if day30="" then rel_com30=1; else rel_com30=0;
	if day90="" then rel_com90=1; else rel_com90=0;
	if day180="" then rel_com180=1; else rel_com180=0;

	/*Make some date flags - indicate which quarter a discharge occurred in*/
	qtr=intck('qtr',&start_dt.,disch)+1;
	%macro qtr_ind;
		%do i=1 %to &max_qtr.;
			q&i.=(intck('qtr',&start_dt.,disch)+1=&i.);
		%end;
	%mend;
	%qtr_ind;
run;

proc freq data=freq_array;
	tables MDS STA SNF LTC IRF HHA DEAD COM com30 rel_com30 rel_com90 rel_com180 radm30 radm45 dd30;
run;

proc freq data=freq_array;
	tables year*(MDS STA SNF LTC IRF HHA);
run;

proc freq data=freq_array;
	tables qtr q1*year q5*year;
run;

proc contents data=freq_array; run;

proc sort data=freq_array out=R.freq_array_&CONDITION.; by PROVID year; run;




%macro out;
%macro means(fac);
proc means data=freq_array;
	class qtr;
	var tot_&fac.;
	where &fac.=1;
	output out=means_&fac.;
run;

proc contents data=means_&fac.; run;
proc print data=means_&fac.; run;

data means_&fac.;
	set means_&fac.;
	if qtr~=. & _TYPE_=1 & trim(left(_STAT_))="MEAN";
run;

proc transpose data=means_&fac. out=means_wide_&fac. prefix=qtr;
    *by qtr;
    id qtr;
    var tot_&fac.;
run;

proc print data=means_wide_&fac.; run;
%mend;
%means(MDS);
%means(IRF);
%means(STA);
%means(COM);

%macro means2(var);
proc means data=freq_array;
	class qtr;
	var &var.;
	output out=means_&var.;
run;

proc contents data=means_&var.; run;
proc print data=means_&var.; run;

data means_&var.;
	set means_&var.;
	if qtr~=. & _TYPE_=1 & trim(left(_STAT_))="MEAN";
run;

proc transpose data=means_&var. out=means_wide_&var._2 prefix=qtr;
    *by qtr;
    id qtr;
    var &var.;
run;

proc print data=means_wide_&var._2; run;
%mend;
%means2(MDS);
%means2(IRF);
%means2(STA);
%means2(COM);
%means2(radm30);
%means2(dd30);

data qtr_table;
	set means_wide_:;
run;

proc print data=qtr_table; run;

proc freq data=freq_array;
	tables qtr*(MDS IRF STA COM radm30 dd30) / chisq;
run;

proc sort data=freq_array out=R.freq_array_&CONDITION.; by PROVID year; run;

/*********************
PRINT CHECK
*********************/
/*Output a sample of each of the datasets to an excel workbook*/
ods tagsets.excelxp file="&out./quarterly_table_&CONDITION..xml" style=sansPrinter;
ods tagsets.excelxp options(absolute_column_width='20' sheet_name="Measures by Quarter" frozen_headers='yes');
proc print data=qtr_table;
run;
ods tagsets.excelxp close;
/*********************
CHECK END
*********************/
%mend;
