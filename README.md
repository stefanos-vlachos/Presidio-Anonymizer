# RoG§: A Pipeline for Automated Sensitive Data Identification and Anonymisation
> § The Ring of Gyges was a magic ring mentioned by the philosopher Plato; it gives its owner the power to become invisible at will.

<p align="center">
  <img src="Platon_Cave_Sanraedam.jpg" />
</p>

Nowadays, the amount of data available online is constantly increasing. This data may contain sensitive or private information that can expose the person behind the data or be misused by malicious actors for identity theft, stalking, and other nefarious purposes. Thus, there is a growing need to protect individuals’ privacy and prevent data breaches in several application domains. This is where **RoG** comes in.

## What is RoG?
Protecting data privacy is a complex and multifaceted process which involves a range of legal, ethical, and technical considerations. Protecting
sensitive data is not trivial, as there are two significant challenges: 
1) ***Data Volume***: A vast amount of data is generated
2) ***Data Diversity***: Data in various formats is produced

It is though, difficult to manually identify and protect all types of sensitive data. Therefore, after assessing the challenges associated with data protection, the role of automated tools, and the effectiveness of identifying and anonymising sensitive data, we came up with **RoG**, a **fully-automated** pipeline for ***sensitive data identification*** and ***anonymisation***.

The *usefulness* of the RoG anonymization pipeline lies in the following features:
* ***Fully-automated anonymization***: RoG requires no or minimum customization
* ***No data science knowledge required***: RoG provides a layer of abstraction over more intricate analysis and anonymization processes
* ***Effective on a wide range of domains***: RoG incorporates versatile PII entity recognition methods 


## A deeper look into RoG
Based on the existing literature and taking into consideration the breadth of the entity recognition and data anonymisation research fields, our interest was steered towards developing a NLP-based pipeline, which would automatically identify personal/sensitive information in a given dataset and facilitate its
anonymisation. To this end, (i) the ***Presidio Analyzer***, along with (ii) the ***Amnesia API*** were identified as two suitable candidates to be used in the proposed approach. 

### Pipeline Architecture
### Sensitive Data Identification
Sensitive data identification involves identifying and categorising data based on its potential privacy implications.

### Sensitive Data Anonymization
Data anonymization refers to the process by which personal data is altered in such a way that a data subject can no longer be identified directly or indirectly, either by the data controller alone or in collaboration with any other party. Data anonymisation may include methods such as ***masking***, ***generalization***, and ***perturbation***, which help to remove or obfuscate personally identifiable information.

### End-to-End Workflow

## User Manual
### How to install RoG?
### How to use RoG?

## Future Work
This work can be considered as a first step towards building a fully-featured anonymisation application, with which users will interact via a friendly interface and will be able to fully automatically and efficiently anonymise any given dataset.

## Contribution
