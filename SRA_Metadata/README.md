# SRA Metadata

A tool used to generate metadata for NCBI [SRA](https://www.ncbi.nlm.nih.gov/sra) submissions by scraping files from GenomeArk.

## Dependencies
- boto3

## Usage
Single entry:
```
python SRA_metadata.py -g single -b [BioSample access] -t [ToLID] -s [Species_name]
```

Batch entry:
```
python SRA_metadata.py -g batch -f [csv filepath]
```

Sample .csv:
| ToLID  |  BioSample  |     Species           |
|--------|-------------|-----------------------|
|fFunDia1|SAMN39736529 |Fundulus diaphanus     |
|bAptMan1|SAMN39257886 |Apteryx mantelli       |
|fChaTrf1|SAMN41253346 |Chaetodon trifascialis |
