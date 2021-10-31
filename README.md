## Bulk Webhook

Bulk Webhook allows creating webhook that sends multiple records and also reports from ERPNext. It kinds of combines Auto-email Reports and Webhook.  
It also extends to allowing writing own scripts to be processed and the data thereof can be used to send via webhook like functionality to another site. We have used it to send to kafka.  
It has scheduling facility of "Every 5 minutes", "Every 15 minutes", "Every 30 minutes", "Hourly", "Daily", "Weekly" or "Monthly".  
It has defaults that are defined at "Bulk Webhook Settings" level so that you may not need to repeat in each of the webhook if you have kafka like scenario where the authentication header could be common.  

#### License  
  
MIT  
