# gdprSHARK

The gdprSHARK is a smartSHARK plugin for the replacement of email addresses in order to raise the compliance to the _General Data Protection Regulation_ (GDPR).

It can be used to search in specified collections and fields of the database for valid email addresses, 
compare them with the email addresses available in the people collection and finally replace found email addresses with a tag.
This tag includes the database id of the person. 
Therefore, it is a pseudonymisation step without loss of information, which can be useful for publishing data (excluding the personal data).

A replacement can look like the following: <code>john.doe@domain.com</code> -> <code>[email:1243abcd1234efgh123456ab]</code>.
If there are multiple entries associated to that email address, the people's ids are listed comma-separated in the tag.
This is done up to a limit of 10 entries. Email addresses associated with more than 10 entries are considered as non-personal email addresses which do not need to be replaced.

The <code>-fields</code> control parameter takes a string with a comma-separated list containing the database fields to search for email addresses to replace.
Its default value is:
<code>"commit.message,message.body,message.subject,issue.desc,issue_comment.comment,pull_request.description,pull_request_commit.message,pull_request_comment.comment"</code>
