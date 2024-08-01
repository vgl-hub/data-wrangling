import boto3
import os
import csv
import re

def get_s3_dirs(prefix, bucket = "genomeark"):
    s3 = boto3.client("s3")
    objects = s3.list_objects(Bucket = bucket, Prefix = prefix, Delimiter = "/")
    if "CommonPrefixes" in objects.keys():
        dirs = [object['Prefix'] for object in objects["CommonPrefixes"]]
    else:
        dirs = []
    return dirs

def get_s3_files(prefix, bucket = "genomeark"):
    s3 = boto3.client("s3")
    objects = s3.list_objects(Bucket = bucket, Prefix = prefix, Delimiter = '/')['Contents']
    filepaths = [object['Key'] for object in objects]
    return filepaths

metadata_dict = {
    '5mC': {
        'library':'PacBio_HiFi_5mC_bam',
        'title':'PacBio HiFi sequencing',
        'library_strategy':'WGS',
        'library_source':'GENOMIC',
        'library_selection':'size fractionation',
        'library_layout':'single',
        'platform':'PACBIO_SMRT',
        'instrument_model':'Sequel II',
        'design_description':'SAGE blue pippin',
        'filetype':'bam',
        'assembly':'unaligned'
    },    
    'reads': {
        'library':'PacBio_reads',
        'title':'PacBio sequencing',
        'library_strategy':'WGS',
        'library_source':'GENOMIC',
        'library_selection':'size fractionation',
        'library_layout':'single',
        'platform':'PACBIO_SMRT',
        'instrument_model':'Sequel II',
        'design_description':'SAGE blue pippin',
        'filetype':'bam',
        'assembly':'unaligned'
    },    
    'subreads': {
        'library':'PacBio_HiFi_subreads',
        'title':'PacBio HiFi sequencing',
        'library_strategy':'WGS',
        'library_source':'GENOMIC',
        'library_selection':'size fractionation',
        'library_layout':'single',
        'platform':'PACBIO_SMRT',
        'instrument_model':'Sequel II',
        'design_description':'SAGE blue pippin',
        'filetype':'bam',
        'assembly':'unaligned'
    },    
    'demultiplex_bam': {
        'library':'PacBio_HiFi_demux_bam',
        'title':'PacBio HiFi sequencing',
        'library_strategy':'WGS',
        'library_source':'GENOMIC',
        'library_selection':'size fractionation',
        'library_layout':'single',
        'platform':'PACBIO_SMRT',
        'instrument_model':'Sequel II',
        'design_description':'SAGE blue pippin',
        'filetype':'bam',
        'assembly':'unaligned'
    },    
    'hifi_bam': {
        'library':'PacBio_HiFi_bam',
        'title':'PacBio HiFi sequencing',
        'library_strategy':'WGS',
        'library_source':'GENOMIC',
        'library_selection':'size fractionation',
        'library_layout':'single',
        'platform':'PACBIO_SMRT',
        'instrument_model':'Revio',
        'design_description':'PippinHT size selection',
        'filetype':'bam',
        'assembly':'unaligned'
    },    
    'fastq': {
        'library':'PacBio_HiFi_fastq',
        'title':'PacBio HiFi sequencing',
        'library_strategy':'WGS',
        'library_source':'GENOMIC',
        'library_selection':'size fractionation',
        'library_layout':'single',
        'platform':'PACBIO_SMRT',
        'instrument_model':'Sequel II',
        'design_description':'SAGE blue pippin',
        'filetype':'fastq.gz',
        'assembly':''
    },   
    '10x': {
        'library':'10x',
        'title':'whole genome sequencing with 10x linked reads',
        'library_strategy':'WGS',
        'library_source':'GENOMIC',
        'library_selection':'RANDOM',
        'library_layout':'paired',
        'platform':'ILLUMINA',
        'instrument_model':'Illumina NovaSeq 6000',
        'design_description':"10x manufacturer\'s protocol",
        'filetype':'fastq.gz',
        'assembly':'unaligned'
    },
    'arima': {
        'library':'HiC',
        'title':'Hi-C sequencing',
        'library_strategy':'Hi-C',
        'library_source':'GENOMIC',
        'library_selection':'Restriction Digest',
        'library_layout':'paired',
        'platform':'ILLUMINA',
        'instrument_model':'Illumina NovaSeq 6000',
        'design_description':"Arima Hi-C manufacturer\'s protocol",
        'filetype':'fastq.gz',
        'assembly':'unaligned'
    },
    'illumina': {
        'library':'Illumina',
        'title':'whole genome shotgun sequencing with Illumina reads',
        'library_strategy':'WGS',
        'library_source':'GENOMIC',
        'library_selection':'RANDOM',
        'library_layout':'paired',
        'platform':'ILLUMINA',
        'instrument_model':'Illumina NovaSeq 6000',
        'design_description':"manufacturer\'s protocol",
        'filetype':'fastq.gz',
        'assembly':'unaligned'
    },
    'dovetail': {
        'library':'HiC',
        'title':'Hi-C sequencing',
        'library_strategy':'Hi-C',
        'library_source':'GENOMIC',
        'library_selection':'Restriction Digest',
        'library_layout':'paired',
        'platform':'ILLUMINA',
        'instrument_model':'Illumina NovaSeq 6000',
        'design_description':"Omni-C Dovetail manufacturer\'s protocol",
        'filetype':'fastq.gz',
        'assembly':'unaligned'
    },
    'element': {
        'library':'HiC',
        'title':'Hi-C sequencing',
        'library_strategy':'Hi-C',
        'library_source':'GENOMIC',
        'library_selection':'Restriction Digest',
        'library_layout':'paired',
        'platform':'ILLUMINA',
        'instrument_model':'Illumina NovaSeq 6000',
        'design_description':"Element Biosciences manufacturer\'s protocol",
        'filetype':'fastq.gz',
        'assembly':'unaligned'
    },
    'RNA-Seq': {
        'library':'RNAseq',
        'title':'RNA',
        'library_strategy':'RNA-Seq',
        'library_source':'TRANSCRIPTOMIC',
        'library_selection':'PolyA',
        'library_layout':'paired',
        'platform':'ILLUMINA',
        'instrument_model':'Illumina NovaSeq 6000',
        'design_description':'RNAseq data generated with Illumina Stranded mRNA Prep',
        'filetype':'fastq'
    },
    'Iso-Seq': {
        'library':'isoseq',
        'title':'Iso-Seq FLNC',
        'library_strategy':'OTHER',
        'library_source':'OTHER',
        'library_selection':'cDNA_oligo_dT',
        'library_layout':'single',
        'platform':'PACBIO_SMRT',
        'instrument_model':'Sequel IIe',
        'design_description':'ISO-seq data generated with PacBio SMRTbell prep kit 3.0',
        'filetype':'bam'
    },    
}

s3 = boto3.client('s3')

filetypes = ['bam', 'fastq.gz', 'fq.gz', 'fasta.gz', 'fa.gz']
pacbio_instruments = {'m54306Ue':'Sequel II',
                   'm64330e':'Sequel II',
                   'm64334e':'Sequel II',
                   'm64055e':'Sequel II',
                   'm84091':'Revio'}

re_extensions = [re.compile('.*.fastq.gz$'), re.compile('.*.bam$')]

re_patterns = {'fastq': re.compile('.*hifi_reads\\..*fastq.gz$'),
               'demultiplex_bam': re.compile('.*bc[0-9]{4}--bc[0-9]{4}\\.bam$'),
               'subreads': re.compile('.*subreads\\.bam$'),
               'reads': re.compile('.*\\.reads\\.bam$'),
               '5mC': re.compile('.*with_5mC\\.bam$'),
               'hifi_bam': re.compile('.*hifi_reads\\.(bc[0-9]{4}\\.)?bam$')}

def get_HiC_filepath_pairs(filepaths):
    filepaths
    pairs = []
    while filepaths:
        filename = filepaths[0]
        extension = filename.split('.', 1)[-1]
        if extension == 'fastq.gz':
            forward_read = filename
            reverse_read = forward_read.replace('R1', 'R2')
            if reverse_read == forward_read:
                reverse_read = forward_read.replace('_1', '_2')
            pairs.append((forward_read, reverse_read))
            filepaths.pop(0)
            filepaths.pop(filepaths.index(reverse_read))
        else:
            filepaths.pop(0)
    return pairs

def get_genome_metadata(species, tolid, biosample_accession):
    file_metadata = dict()
    prefix = f'species/%s/%s/genomic_data/' % (species.replace(' ', '_'), tolid)
    dirs = get_s3_dirs(prefix)
    species = species.replace('_', ' ')
    if not dirs:
        print(f'Genome sequence data for %s, %s not found in GenomeArk.' % (species, tolid))
    else:
        hic_pairs_count = 1
        for subdir in dirs:
            platform = subdir.split('/')[-2]
            filepaths = get_s3_files(prefix = subdir)
            filepaths = [filepath.split('/')[-1] for filepath in filepaths if '.bam' in filepath.split('/')[-1] or '.fastq.gz' in filepath.split('/')[-1]]
            if platform in ['arima', 'dovetail', '10X', '10x']:
                filepath_pairs = get_HiC_filepath_pairs(filepaths)
                for pair in filepath_pairs:
                    metadata = metadata_dict[platform.lower()]
                    file_metadata[pair[0]] = {'biosample_accession': biosample_accession, 
                                        'library_ID': f'%s_%s_%i' % (tolid, metadata['library'], hic_pairs_count), 
                                        'title': f'%s %s' % (species, metadata['title']), 
                                        'library_strategy': metadata['library_strategy'], 
                                        'library_source': metadata['library_source'], 
                                        'library_selection': metadata['library_selection'],
                                        'library_layout': metadata['library_layout'], 
                                        'platform': metadata['platform'], 
                                        'instrument_model': metadata['instrument_model'], 
                                        'design_description': metadata['design_description'], 
                                        'filetype': 'fastq',
                                        'filename': f'%s%s' % (subdir, pair[0]), 
                                        'filename2': f'%s%s' % (subdir, pair[1]),
                                        'filename3':'',
                                        'filename4':'',
                                        'assembly':'',
                                        'fasta_file':'',
                                        }
                    hic_pairs_count += 1
            if platform == "illumina":
                filepath_pairs = get_HiC_filepath_pairs(filepaths)
                for pair in filepath_pairs:
                    metadata = metadata_dict[platform.lower()]
                    file_metadata[pair[0]] = {'biosample_accession': biosample_accession, 
                                        'library_ID': f'%s_%s_%i' % (tolid, metadata['library'], filepath_pairs.index(pair) + 1), 
                                        'title': f'%s %s' % (species, metadata['title']), 
                                        'library_strategy': metadata['library_strategy'], 
                                        'library_source': metadata['library_source'], 
                                        'library_selection': metadata['library_selection'],
                                        'library_layout': metadata['library_layout'], 
                                        'platform': metadata['platform'], 
                                        'instrument_model': metadata['instrument_model'], 
                                        'design_description': metadata['design_description'], 
                                        'filetype': 'fastq',
                                        'filename': f'%s%s' % (subdir, pair[0]), 
                                        'filename2': f'%s%s' % (subdir, pair[1]),
                                        'filename3':'',
                                        'filename4':'',
                                        'assembly':'',
                                        'fasta_file':'',
                                        }
            elif platform == "pacbio_hifi":
                filetype_counts = {
                    'fastq': 1,
                    'demultiplex_bam': 1,
                    'subreads': 1,
                    'reads':1,
                    '5mC': 1,
                    'hifi_bam': 1
                }
                for filepath in filepaths:
                    extension = filepath.split('.', 1)[1]
                    metadata_tag = filepath.split('.', 1)[0]
                    instrument_number = metadata_tag.split('_')[0]
                    if instrument_number in pacbio_instruments.keys():
                        instrument = pacbio_instruments[instrument_number]
                    else:
                        instrument = 'Sequel II'
                    filetype = ''
                    if '.fastq.gz' in extension and 'hifi_reads' in extension:
                        filetype = 'fastq'
                    elif '.bam' in extension:
                        filetype = [type for type in re_patterns.keys() if re.match(re_patterns[type], filepath)]
                        if filetype:
                            filetype = filetype[0]
                    if filetype:
                        metadata = metadata_dict[filetype]
                        file_metadata[filepath] = {'biosample_accession': biosample_accession, 
                                        'library_ID': f'%s_%s_%i' % (tolid, metadata['library'], filetype_counts[filetype]), 
                                        'title': f'%s %s' % (species, metadata['title']), 
                                        'library_strategy': metadata['library_strategy'], 
                                        'library_source': metadata['library_source'], 
                                        'library_selection': metadata['library_selection'],
                                        'library_layout': metadata['library_layout'], 
                                        'platform': metadata['platform'], 
                                        'instrument_model': instrument, 
                                        'design_description': 'SAGE blue pippin' if instrument == 'Sequel II' else 'PippinHT size selection',
                                        'filetype': 'fastq' if filetype == 'fastq' else 'bam',
                                        'filename': f'%s%s' % (subdir, filepath),
                                        'filename2':'',
                                        'filename3':'',
                                        'filename4':'',
                                        'assembly':metadata['assembly'],
                                        'fasta_file':'',
                                        }
                        filetype_counts[filetype] += 1
    return file_metadata

def get_transcriptome_metadata(species, tolid, biosample_accession):
    file_metadata = dict()
    prefix = f'species/%s/%s/transcriptomic_data/' % (species.replace(' ', '_'), tolid)
    dirs = get_s3_dirs(prefix)
    species = species.replace('_', ' ')
    if not dirs:
        print(f'Transcriptome data for %s, %s not found in GenomeArk.' % (species, tolid))
        return file_metadata
    else:
        for subdir in dirs:
            tissue = subdir.split('/')[-2]
            tissue_name = tissue.replace('_', ' ').capitalize()
            platforms = [platform.split('/')[-2] for platform in get_s3_dirs(prefix = subdir)]
            for platform in platforms:
                filepaths = get_s3_files(prefix = f'%s%s/' % (subdir, platform))
                if platform == 'illumina':
                    filepath_pairs = get_HiC_filepath_pairs(filepaths)
                    for pair in filepath_pairs:
                        metadata = metadata_dict['RNA-Seq']
                        file_metadata[pair[0]] = {'biosample_accession': biosample_accession, 
                                            'library_ID': f'%s_%s_%s' % (tolid, metadata['library'], tissue), 
                                            'title': f'%s %s %s' % (species, tissue_name, metadata['title']), 
                                            'library_strategy': metadata['library_strategy'], 
                                            'library_source': metadata['library_source'], 
                                            'library_selection': metadata['library_selection'],
                                            'library_layout': metadata['library_layout'], 
                                            'platform': metadata['platform'], 
                                            'instrument_model': metadata['instrument_model'], 
                                            'design_description': metadata['design_description'], 
                                            'filetype': 'fastq',
                                            'filename': pair[0], 
                                            'filename2': pair[1],
                                            'filename3':'',
                                            'filename4':'',
                                            'assembly':'',
                                            'fasta_file':'',
                                            }
                elif platform == 'pacbio_hifi':
                    for filepath in filepaths:
                        file = filepath.split('/')[-1]
                        extension = file.split(os.extsep, 1)[-1]
                        if extension == 'bam':
                            metadata = metadata_dict['Iso-Seq']
                            file_metadata[file] = {'biosample_accession': biosample_accession, 
                                                'library_ID': f'%s_%s' % (tolid, metadata['library']), 
                                                'title': f'%s %s %s' % (species, tissue.capitalize(), metadata['title']), 
                                                'library_strategy': metadata['library_strategy'], 
                                                'library_source': metadata['library_source'], 
                                                'library_selection': metadata['library_selection'],
                                                'library_layout': metadata['library_layout'], 
                                                'platform': metadata['platform'], 
                                                'instrument_model': metadata['instrument_model'], 
                                                'design_description': metadata['design_description'], 
                                                'filetype': 'bam',
                                                'filename': filepath, 
                                                'filename2': '',
                                                'filename3':'',
                                                'filename4':'',
                                                'assembly':'unaligned',
                                                'fasta_file':'',
                                                }
    return file_metadata

def write_file_metadata(file_metadata, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    print(f'Metadata saved to %s' % (filename))
    with open(filename, mode = 'w') as out:
        fieldnames = ['biosample_accession', 'library_ID', 'title', 'library_strategy', 'library_source', 'library_selection',
                'library_layout', 'platform', 'instrument_model', 'design_description', 'filetype', 'filename', 'filename2',
                'filename3', 'filename4', 'assembly', 'fasta_file']
        writer = csv.DictWriter(out, delimiter ='\t', fieldnames = fieldnames)
        writer.writeheader()
        for key in file_metadata.keys():
            line = file_metadata[key]
            writer.writerow(line)
