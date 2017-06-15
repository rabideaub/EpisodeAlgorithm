libname in "/schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Data/MainAnalysis/Final";
%let out = /schaeffer-b/sch-protected/from-projects/VERTICAL-INTEGRATION/rabideau/Output;

data all_cond;
	set in.freq_array_sk_drg_snf (in=a);
	if a then cond='sk_drg';

	if 65<=age<70 then agecat='65-69';
	if 70<=age<75 then agecat='70-74';
	if 75<=age<80 then agecat='75-79';
	if 80<=age<85 then agecat='80-84';
	if 85<=age<90 then agecat='85-89';
	if age>=90 then agecat='90+';

	index_los=(disch-admit)+1;

	dd180=(day180="DEAD");
	all_com=(tot_COM=180);
run;

%macro yearly_summary(dx);
	%macro loop_var(var,num);
		ods output
		CrossTabFreqs = xtab;
		proc freq data=all_cond;
			tables (&var.)*year 
			/ missing out=y_&var._&dx.;
			where cond="&dx.";
		run;
		ods output close;

		data y_&var._&dx.;
			set xtab (keep=&var. year colpercent where=(colpercent~=.));
			if colpercent <.01 then colpercent=.;
		run;

		proc print data=y_&var._&dx.; run;

		proc transpose data=y_&var._&dx. out=y_&var._&dx. prefix=Y;
			by &var.;
			id year;
			var colpercent;
		run;

		data y_&dx._&var. (drop=_NAME_ _LABEL_ &var.);
			length condition $2 variable $32 value $32;
			set y_&var._&dx.;
			condition="&dx.";
			variable="&var.";
			value=put(&var.,$32.);
			sort_order=&num.;
			if trim(left(value))='0' then delete; 
			run;

		proc print data=y_&var._&dx.; run;
	%mend;
	%loop_var(var=race, num=1); %loop_var(var=agecat, num=2); %loop_var(var=male, num=3); %loop_var(var=radm30, num=4); %loop_var(var=radm45, num=5); %loop_var(var=dd30, num=6); %loop_var(var=dd180, num=7); 
	%loop_var(var=all_com, num=9); %loop_var(var=IRF, num=10); %loop_var(var=SNF, num=11); %loop_var(var=LTC, num=12); %loop_var(var=HHA, num=13);  %loop_var(var=MDS, num=14);  
	%loop_var(var=CHF,num=35); %loop_var(var=VALVE,num=36); %loop_var(var=PULMCIRC,num=37); %loop_var(var=PERIVASC,num=38); %loop_var(var=HTN_C,num=39); %loop_var(var=PARA,num=40); %loop_var(var=NEURO,num=41); 
	%loop_var(var=CHRNLUNG,num=42); %loop_var(var=DM,num=43); %loop_var(var=DMCX,num=44); %loop_var(var=HYPOTHY,num=45); %loop_var(var=RENLFAIL,num=46); %loop_var(var=LIVER,num=47); %loop_var(var=ULCER,num=48); 
	%loop_var(var=AIDS,num=49); %loop_var(var=LYMPH,num=50); %loop_var(var=METS,num=51); %loop_var(var=TUMOR,num=52); %loop_var(var=ARTH,num=53); %loop_var(var=COAG,num=54); %loop_var(var=OBESE,num=55); 
	%loop_var(var=WGHTLOSS,num=56); %loop_var(var=LYTES,num=57); %loop_var(var=BLDLOSS,num=58); %loop_var(var=ANEMDEF,num=59); %loop_var(var=ALCOHOL,num=60); %loop_var(var=DRUG,num=61);
	%loop_var(var=PSYCH,num=62); %loop_var(var=DEPRESS,num=63); %loop_var(var=elix_cnt, num=64);

	data yearly_all_&dx.;
		set y_&dx.:;
	run;

	proc sort data=yearly_all_&dx.; by condition variable value; run;

	data yearly_all_&dx.;
		set yearly_all_&dx.;
		by condition variable;
		if ~first.variable then variable='';
	run;

	proc sort data=yearly_all_&dx.; by condition sort_order descending variable value; run;
%mend;
%yearly_summary(sk_drg);


%macro qtr_summary(dx);
		ods output
		CrossTabFreqs = xtab;
		proc freq data=all_cond;
			tables (radm30)*qtr 
			/ missing out=q_radm30_&dx.;
			where cond="&dx.";
		run;
		ods output close;

		data q_radm30_&dx.;
			set xtab (keep=radm30 qtr colpercent where=(colpercent~=.));
			if colpercent <.01 then colpercent=.;
		run;

		proc print data=q_radm30_&dx.; run;

		proc transpose data=q_radm30_&dx. out=q_radm30_&dx. prefix=Q;
			by radm30;
			id qtr;
			var colpercent;
		run;

		data q_radm30_&dx. (drop=_NAME_ _LABEL_);
			length condition $2;
			set q_radm30_&dx.;
			condition="&dx.";
		run;

		proc print data=q_radm30_&dx.; run;
%mend;
%qtr_summary(sk_drg);

data summary_year;
	set yearly_all_:;
run;

data summary_qtr;
	set q_radm30_:;
run;

proc print data=summary_year; run;
proc print data=summary_qtr; run;

proc means data=all_cond;
	class year;
	var age index_los tot_SNF tot_IRF tot_STA tot_HHA tot_LTC tot_COM;
run;

ods tagsets.excelxp file="&out./mainanalysis_summary_safs.xml" style=sansPrinter;
ods tagsets.excelxp options(absolute_column_width='20' sheet_name="Yearly Summary" frozen_headers='yes');
proc print data=summary_year;
run;
ods tagsets.excelxp options(absolute_column_width='20' sheet_name="Quarterly Summary" frozen_headers='yes');
proc print data=summary_qtr;
run;
ods tagsets.excelxp close;

