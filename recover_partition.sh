curl  -i -X POST -H "X-AUTH-TOKEN: dd8a85342daa48d7bfc5a2c3d6ea2c703273111e0cca4708ae086556081acff5" -H "Content-Type: application/json" -H "Accept: application/json" \
-d '{
       "query":"alter table cross_dsp_insights_tableau.report_tableau_feed_cross_dsp_campaign recover partitions;",
       "command_type": "HiveCommand",
       "label":"TABLEAU_DAILY"
    }' \
"https://us.qubole.com/api/v1.2/commands";

curl  -i -X POST -H "X-AUTH-TOKEN: dd8a85342daa48d7bfc5a2c3d6ea2c703273111e0cca4708ae086556081acff5" -H "Content-Type: application/json" -H "Accept: application/json" \
-d '{
       "query":"alter table cross_dsp_insights_tableau.report_geo_cross_dsp_campaign recover partitions;",
       "command_type": "HiveCommand",
       "label":"TABLEAU_DAILY"
    }' \
"https://us.qubole.com/api/v1.2/commands";

curl  -i -X POST -H "X-AUTH-TOKEN: dd8a85342daa48d7bfc5a2c3d6ea2c703273111e0cca4708ae086556081acff5" -H "Content-Type: application/json" -H "Accept: application/json" \
-d '{
       "query":"alter table cross_dsp_insights_tableau.report_sd_insights_cross_dsp_campaign recover partitions;",
       "command_type": "HiveCommand",
       "label":"TABLEAU_DAILY"
    }' \
"https://us.qubole.com/api/v1.2/commands";

curl  -i -X POST -H "X-AUTH-TOKEN: dd8a85342daa48d7bfc5a2c3d6ea2c703273111e0cca4708ae086556081acff5" -H "Content-Type: application/json" -H "Accept: application/json" \
-d '{
       "query":"alter table cross_dsp_insights_tableau.report_timelag_tz_cross_dsp_campaign recover partitions;",
       "command_type": "HiveCommand",
       "label":"TABLEAU_DAILY"
    }' \
"https://us.qubole.com/api/v1.2/commands";

curl  -i -X POST -H "X-AUTH-TOKEN: dd8a85342daa48d7bfc5a2c3d6ea2c703273111e0cca4708ae086556081acff5" -H "Content-Type: application/json" -H "Accept: application/json" \
-d '{
       "query":"alter table cross_dsp_insights_tableau.report_url_keyword_cross_dsp_campaign recover partitions;",
       "command_type": "HiveCommand",
       "label":"TABLEAU_DAILY"
    }' \
"https://us.qubole.com/api/v1.2/commands";

curl  -i -X POST -H "X-AUTH-TOKEN: dd8a85342daa48d7bfc5a2c3d6ea2c703273111e0cca4708ae086556081acff5" -H "Content-Type: application/json" -H "Accept: application/json" \
-d '{
       "query":"alter table cross_dsp_insights_tableau.tableau_feed_cross_dsp_lkup recover partitions;",
       "command_type": "HiveCommand",
       "label":"TABLEAU_DAILY"
    }' \
"https://us.qubole.com/api/v1.2/commands";

sleep 2400
