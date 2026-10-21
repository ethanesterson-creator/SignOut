# Getting ethan@headcountcamp.com

Why: cold emails to camp directors from a personal Gmail look like a side project and are more likely
to land in spam. An address on your own domain looks like a business. Pick ONE of the three below.

## A. Google Workspace Business Starter (simplest, best delivery) about $7 per month on an annual plan ($8.40 monthly)
Check the live price at workspace.google.com/pricing. You get a real Gmail inbox at ethan@headcountcamp.com.
1. Go to workspace.google.com, choose Business Starter, and start the free trial.
2. Enter `headcountcamp.com` as your domain. Google gives you a TXT record to prove you own it.
3. At the place where you bought the domain, open DNS and add that TXT record. Click Verify in Google.
4. Google then asks you to add MX records (they are listed on screen). Add them in the same DNS screen.
5. In the Admin console, go to Apps > Google Workspace > Gmail > Authenticate email and turn on DKIM
   (it gives you one more TXT record to add). Also add an SPF TXT record: `v=spf1 include:_spf.google.com ~all`.
6. Create your mailbox `ethan@headcountcamp.com` and send yourself a test.

## B. Porkbun email hosting (cheapest, only if the domain is at Porkbun) $36 per mailbox per year
Check the live price on Porkbun. Each domain gets a free 15-day trial.
1. Porkbun > Domain Management > headcountcamp.com > Email > add a mailbox `ethan`.
2. Porkbun fills in the MX, SPF and DKIM records for you when the domain uses Porkbun DNS.
3. Use their webmail, or add it to your phone and to Gmail (IMAP imap.porkbun.com 993, SMTP smtp.porkbun.com 587).

## C. Free forwarding only (not recommended for cold email)
Cloudflare Email Routing can forward ethan@headcountcamp.com into Gmail for free, but it cannot send. Replying "as"
the domain address through Gmail needs workarounds and often shows "via gmail.com", which is what we are trying to avoid.

## After it works
Tell me the new address. I will swap it into the website's contact links and the email drafts, and the demo
site can use it too. Send a test email to a friend's Gmail and to an Outlook address and check it is not in spam
before the first outreach email.
