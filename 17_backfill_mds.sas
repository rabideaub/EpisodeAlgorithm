/*The purpose of this program is to flatten out the MDS, merge it onto the day array, and fill in MDS
  stays using the RHF approach of backfilling days based on the Intrator paper (https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3015013/)*/

libname raw "/schaeffer-b/sch-protected/VERTICAL-INTEGRATION/Data/2009-13_PAC/Raw/MDS";
libname med "/schaeffer-b/sch-protected/VERTICAL-INTEGRATION/Data/2009-13_PAC/Raw/MedPAR";
libname final "/schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Data/Romley/Final";
libname temp "/schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Data/Romley/Temp";

data mds2;
	set raw.mds2006 (keep=bene_id target_date AA8A_PRI_RFA aa8b_spc_rfa R4_DISCHARGE_DT rename=(target_date=target_dt R4_DISCHARGE_DT=disch_dt))
		raw.mds2007 (keep=bene_id target_date AA8A_PRI_RFA aa8b_spc_rfa R4_DISCHARGE_DT rename=(target_date=target_dt R4_DISCHARGE_DT=disch_dt))
		raw.mds2008 (keep=bene_id target_date AA8A_PRI_RFA aa8b_spc_rfa R4_DISCHARGE_DT rename=(target_date=target_dt R4_DISCHARGE_DT=disch_dt))
		raw.mds2009 (keep=bene_id target_date AA8A_PRI_RFA aa8b_spc_rfa R4_DISCHARGE_DT rename=(target_date=target_dt R4_DISCHARGE_DT=disch_dt))
		raw.mds2010 (keep=bene_id target_date AA8A_PRI_RFA aa8b_spc_rfa R4_DISCHARGE_DT rename=(target_date=target_dt R4_DISCHARGE_DT=disch_dt));

	/*Reason for the Assessment - AA8A_PRI_RFA*/
	/*01=admission
	  02=annual
	  03=change in status
	  04=correction to prior full assessment
	  05=quarterly
	  06=discharge: return not anticipated
	  07=discharge: return anticipated
	  08=discharge prior to initial assessment
	  09=reentry
	  10=correction to quarterly assessment
	  00=none of the abvove*/

	/*Reason for the Assessment - aa8b_spc_rfa*/
	/*1 = Medicare 5 day assessment
	  7 = Medicare 14 day assessment
	  2 = Medicare 30 day assessment
	  3 = Medicare 60 day assessment*/

run;

proc freq data=mds2;
	tables AA8A_PRI_RFA aa8b_spc_rfa / missing;
run;

data mds3;
	set raw.mds_asmt_summary_3_2010 (keep=bene_id trgt_dt A0310A_FED_OBRA_CD a0310b_pps_cd A2000_DSCHRG_DT rename=(trgt_dt=target_dt))
		raw.mds_asmt_summary_3_2011 (keep=bene_id trgt_dt A0310A_FED_OBRA_CD a0310b_pps_cd A2000_DSCHRG_DT rename=(trgt_dt=target_dt))
		raw.mds_asmt_summary_3_2012 (keep=bene_id trgt_dt A0310A_FED_OBRA_CD a0310b_pps_cd A2000_DSCHRG_DT rename=(trgt_dt=target_dt))
		raw.mds_asmt_summary_3_2013 (keep=bene_id trgt_dt A0310A_FED_OBRA_CD a0310b_pps_cd A2000_DSCHRG_DT rename=(trgt_dt=target_dt));

	disch_dt=input(A2000_DSCHRG_DT, yymmdd8.);

	/*Reason for the Assessment - A0310A_FED_OBRA_CD*/
	/*01=admission
	  02=quarterly
	  03=annual
	  04=correction to prior status assessment
	  05=correction to prior full assessment
	  06=correction to prior quarterly assessment
	  99=none of the abvove*/

	/*Reason for the Assessment - a0310b_pps_cd*/
	/*99=None of the above
	  01=5-day scheduled assessment
	  02=14-day scheduled assessment
	  03=30-day scheduled assessment
	  07=Unscheduled assessment used for PPS (OMRA, significant or clinical
	  04=60-day scheduled assessment
	  06=Readmission/return assessment
	  05=90-day scheduled assessment*/

run;

proc freq data=mds3;
	tables A0310A_FED_OBRA_CD a0310b_pps_cd / missing;
run;

data mds;	
	set mds2 (in=a)
		mds3 (in=b);
	/*Standardize the assessment reason*/
	if a then do;
		if AA8A_PRI_RFA='01' then assess_reason='01'; /*Admission Assess*/
		else if AA8A_PRI_RFA='05' then assess_reason='02'; /*Quarterly Assess*/
		else if AA8A_PRI_RFA='02' then assess_reason='03'; /*Annual Assess*/
		else if AA8A_PRI_RFA='99' & aa8b_spc_rfa='1' then assess_reason= '05'; /*5-Day Assessment*/
		else if AA8A_PRI_RFA='99' & aa8b_spc_rfa='7' then assess_reason= '06'; /*14-Day Assessment*/
		else if AA8A_PRI_RFA='99' & aa8b_spc_rfa='2' then assess_reason= '07'; /*30-Day Assessment*/
		else if AA8A_PRI_RFA='99' & aa8b_spc_rfa='3' then assess_reason= '08'; /*60-Day Assessment*/
		else if AA8A_PRI_RFA='99' & aa8b_spc_rfa='4' then assess_reason= '09'; /*90-Day Assessment*/
		else assess_reason='99'; /*Other*/
	end;
	if b then do;
		if A0310A_FED_OBRA_CD='01' then assess_reason='01'; /*Admission Assess*/
		else if A0310A_FED_OBRA_CD='02' then assess_reason='02'; /*Quarterly Assess*/
		else if A0310A_FED_OBRA_CD='03' then assess_reason='03'; /*Annual Assess*/
		else if A0310A_FED_OBRA_CD='99' & a0310b_pps_cd='01' then assess_reason='05'; /*5-Day Assessment*/
		else if A0310A_FED_OBRA_CD='99' & a0310b_pps_cd='02' then assess_reason='06'; /*14-Day Assessment*/
		else if A0310A_FED_OBRA_CD='99' & a0310b_pps_cd='03' then assess_reason='07'; /*30-Day Assessment*/
		else if A0310A_FED_OBRA_CD='99' & a0310b_pps_cd='04' then assess_reason='08'; /*60-Day Assessment*/
		else if A0310A_FED_OBRA_CD='99' & a0310b_pps_cd='05' then assess_reason='09'; /*90-Day Assessment*/
		else assess_reason='99'; /*Other*/
	end;
	if disch_dt~=. then assess_reason='04'; /*Discharge*/
	if assess_reason='' then assess_reason='99';
run;

proc freq data=mds;
	tables assess_reason / missing;
run;

proc sort data=mds; by bene_id target_dt; run;

data mds;
	set mds;
	retain count former_dt;
	by bene_id;
	if first.bene_id then do;
		count=0;
		former_dt=target_dt;
	end;
	count+1;
	if first.bene_id~=1 then do;
		previous_dt=former_dt;
		former_dt=target_dt;
	end;
	format previous_dt former_dt date9.;
run;

data tot_count;
	set mds;
	by bene_id;
	if last.bene_id then output;
run;
	

/*Count the maximum number of assessments per bene. Store this variable as N*/
proc sort data=tot_count; by descending count; run;

data _null_;
	set tot_count;
	if _n_=1 then call symput('N',trim(left(count)));
run;

/*Transpose and combine to create a wide bene-level file with our 3 variables of interest */
proc transpose data=mds out=mds_target_dt prefix=mds_target_dt;
	by bene_id;
	var target_dt;
run;

proc transpose data=mds out=mds_assess_reason prefix=mds_assess_reason;
	by bene_id;
	var assess_reason;
run;

proc transpose data=mds out=mds_previous_dt prefix=mds_previous_dt;
	by bene_id;
	var previous_dt;
run;

data mds_wide (rename=(bene_id=hicno));
	merge mds_target_dt
		  mds_assess_reason
		  mds_previous_dt;
	by bene_id;
run;

title "Look at the Long MDS";
proc print data=mds (obs=100); run;

title "Look at the Wide MDS";
proc print data=mds_wide (obs=10); run;
title;

/*Merge onto the day array and apply rules*/
data day_array;
	set final.freq_array_kh /*(keep=hicno admit disch day:)*/; /*For now just keep key variables*/
	array day {*} $ day1-day180;
	do i=1 to 180;
		if day[i]="MDS" then day[i]=""; /*Set MDS days to missing for now. In the final version this wont be necessary*/
	end;
run;

proc sort data=day_array; by hicno; run;

data day_array;
	merge day_array (in=a)
		  mds_wide (in=b);
	by hicno;
	if a;
	array day {*} $ day1-day180;

	%macro loop_assessments;
		%do i=1 %to &N.;
			start_dt=.;
			stop=0;
			if mds_target_dt&i.>=disch then do; /*Check if assessment comes after index discharge*/
				/*Establish how far back to backfill based on assessment reason. Go back to most recent prior assess, or impute days based on assess type*/
				if mds_assess_reason&i.='01' then start_dt=max(mds_previous_dt&i., (mds_target_dt&i.-14)); /*Admission Assessments must be done within 14 days*/
				else if mds_assess_reason&i.='02' then start_dt=max(mds_previous_dt&i.,(mds_target_dt&i.-92)); /*Quarterly Assessments must be done within 92 days*/
				else if mds_assess_reason&i.='03' then start_dt=max(mds_previous_dt&i., (mds_target_dt&i.-365)); /*Annual Assessments must be done within 365 days*/
				else if mds_assess_reason&i.='04' then start_dt=max(mds_previous_dt&i.,(mds_target_dt&i.-92)); /*For Discharges just fill in back to the most recent assessment, or 92 days back. Whichever is greater*/
				else if mds_assess_reason&i.='05' then start_dt=max(mds_previous_dt&i.,(mds_target_dt&i.-5));  /*5-Day Assessment*/
				else if mds_assess_reason&i.='06' then start_dt=max(mds_previous_dt&i.,(mds_target_dt&i.-14)); /*14-Day Assessment*/
				else if mds_assess_reason&i.='07' then start_dt=max(mds_previous_dt&i.,(mds_target_dt&i.-30)); /*30-Day Assessment*/
				else if mds_assess_reason&i.='08' then start_dt=max(mds_previous_dt&i.,(mds_target_dt&i.-60)); /*60-Day Assessment*/
				else if mds_assess_reason&i.='09' then start_dt=max(mds_previous_dt&i.,(mds_target_dt&i.-90)); /*90-Day Assessment*/
				else if mds_assess_reason&i.='99' then start_dt=mds_target_dt&i.; /*For other assessments, just look at the day of the assessment with no backfill*/
				if start_dt~=. then do; /*If one of the above assessments, proceed with backfilling days*/
					day_start=start_dt-disch; /*Which day in the day array does the backfilling start with*/
					if day_start<1 then day_start=1; /*If the backfilling would go before the start of the day array, start with day 1*/
					if day_start<=180 then do; /*If the day we're interested in is within our episode of care, continue*/
						day_end=mds_target_dt&i.-disch; /*Find the day in the day array that the backilling will end with (date of the assessment)*/
						if day_end>180 then day_end=180; /*If it's going to end outside of our episode, just end it on the last day*/
						do j=day_end to day_start by -1 until(stop); /*Fill in consecutive days. If other PAC in between, stop backfilling*/
							if day[j]="" | day[j]="MDS" then do;
								day[j]="MDS";
								impute_mds=1;
							end;
							else if day[j]~in("","MDS") then stop=1;
						end;
					end;
				end;
			end;
		%end;
	%mend;
	%loop_assessments;
run;

title "Look at the Day Array";
proc print data=day_array(obs=10); run;

title "Look at Imputed MDS Days";
proc print data=day_array(obs=10);
	where impute_mds=1;
run;
title;

data temp.freq_array_kh (drop=mds_target_dt: mds_previous_dt: mds_assess_reason:);
	set day_array;
run;

