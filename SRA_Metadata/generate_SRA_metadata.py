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
    'Kinnex': {
        'library':'kinnex',
        'title':'Kinnex FLNC',
        'library_strategy':'OTHER',
        'library_source':'OTHER',
        'library_selection':'cDNA_oligo_dT',
        'library_layout':'single',
        'platform':'PACBIO_SMRT',
        'instrument_model':'Revio',
        'design_description':'Kinnex data generated with PacBio SMRTbell prep kit 3.0',
        'filetype':'bam'
    },    
}

s3 = boto3.client('s3')

filetypes = ['bam', 'fastq.gz', 'fq.gz', 'fasta.gz', 'fa.gz']
pacbio_instruments = {'m54306Ue':'Sequel II',
                   'm64330e':'Sequel II',
                   'm64334e':'Sequel II',
                   'm64055e':'Sequel II',
                   'm84091':'Revio',
                   'm84203':'Revio'}

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
    part_basenames = set()

    # Exclude files that have been split into parts
    for f in filepaths:
        if 'part' in f:
            base = f.split('_part')[0] + '.' + f.split('.', 1)[-1]
            part_basenames.add(base)

    filepaths = [f for f in filepaths if f not in part_basenames]
    
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

# Matches the read token (R1/R2 or 1/2) set off by _ or . on both sides,
# e.g. fRivCyl1_BR1_S26_R1_001.fastq.gz  or  sample_1.fastq.gz
READ_TOKEN = re.compile(r'^(?P<pre>.*[_.])R?(?P<read>[12])(?P<post>[_.].*)$')

def get_RNASeq_filepaths(filepaths):
    """Split fastq.gz files into paired-end and single-end sets.

    Returns (paired, single):
        paired: list of (R1, R2) tuples
        single: list of R1 filenames with no matching R2
    """
    fastqs = [f for f in filepaths if f.endswith('.fastq.gz')]

    # Exclude original files that have been split into parts (same logic as before)
    part_parents = set()
    for f in fastqs:
        if '_part' in f:
            part_parents.add(f.split('_part')[0] + '.' + f.split('.', 1)[-1])
    fastqs = [f for f in fastqs if f not in part_parents]

    # Group reads by filename with the read number removed
    groups = {}
    for f in fastqs:
        basename = f.split('/')[-1]
        match = READ_TOKEN.match(basename)
        if not match:
            print(f'Warning: could not identify read number in {f}, skipping.')
            continue
        key = f[:len(f) - len(basename)] + match['pre'] + match['post']
        groups.setdefault(key, {})[match['read']] = f

    paired, single = [], []
    for key in sorted(groups):
        reads = groups[key]
        if '1' in reads and '2' in reads:
            paired.append((reads['1'], reads['2']))
        elif '1' in reads:
            single.append(reads['1'])
        else:
            print(f'Warning: R2 without R1 found: {reads["2"]}')
    return paired, single

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
                metadata_tags = []
                for filepath in filepaths:
                    extension = filepath.split('.', 1)[1]
                    metadata_tag = filepath.split('.', 1)[0]
                    instrument_number = metadata_tag.split('_')[0]
                    date_part = metadata_tag.split("_")[1]
                    year = int("20" + date_part[:2])
                    month = int(date_part[2:4])
                    if instrument_number in pacbio_instruments.keys():
                        instrument = pacbio_instruments[instrument_number]
                    else:
                        instrument = 'Sequel II'
                    if instrument == "Revio":
                        if (year, month) > (2025, 2):
                            design_description = "PacBio SPRQ chemistry, PippinHT size selection"
                        else:
                            design_description = "PippinHT size selection"
                    else:
                        design_description = "SAGE blue pippin"
                    filetype = ''
                    if re.match(re_patterns['fastq'], filepath) and 'hifi_reads' in extension:
                        filetype = 'fastq'
                    elif '.bam' in extension:
                        filetype = [type for type in re_patterns.keys() if re.match(re_patterns[type], filepath)]
                        if filetype:
                            filetype = filetype[0]
                    if filetype:
                        metadata = metadata_dict[filetype]
                        if metadata_tag not in metadata_tags:
                            metadata_tags.append(metadata_tag)
                        file_metadata[filepath] = {'biosample_accession': biosample_accession, 
                                        'library_ID': f'%s_%s_%i' % (tolid, metadata['library'], metadata_tags.index(metadata_tag) + 1), 
                                        'title': f'%s %s' % (species, metadata['title']), 
                                        'library_strategy': metadata['library_strategy'], 
                                        'library_source': metadata['library_source'], 
                                        'library_selection': metadata['library_selection'],
                                        'library_layout': metadata['library_layout'], 
                                        'platform': metadata['platform'], 
                                        'instrument_model': instrument, 
                                        'design_description': design_description,
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

def get_transcriptome_metadata(species, tolid, biosample_accession, tissue):
    file_metadata = dict()
    tissue = tissue.strip().lower().replace(' ', '_')   # matches S3 directory naming
    tissue_name = tissue.replace('_', ' ').capitalize()
    prefix = f'species/{species.replace(" ", "_")}/{tolid}/transcriptomic_data/{tissue}/'
    species = species.replace('_', ' ')

    dirs = get_s3_dirs(prefix)
    if not dirs:
        print(f'{tissue_name} transcriptome data for {species}, {tolid} not found in GenomeArk.')
        return file_metadata

    def build_entry(metadata, filetype, filename, filename2='', assembly=''):
        return {
            'biosample_accession': biosample_accession,
            'library_ID': f'{tolid}_{metadata["library"]}_{tissue}',
            'title': f'{species} {tissue_name} {metadata["title"]}',
            'library_strategy': metadata['library_strategy'],
            'library_source': metadata['library_source'],
            'library_selection': metadata['library_selection'],
            'library_layout': metadata['library_layout'],
            'platform': metadata['platform'],
            'instrument_model': metadata['instrument_model'],
            'design_description': metadata['design_description'],
            'filetype': filetype,
            'filename': filename,
            'filename2': filename2,
            'filename3': '',
            'filename4': '',
            'assembly': assembly,
            'fasta_file': '',
        }

    for platform in [d.split('/')[-2] for d in dirs]:
        filepaths = get_s3_files(prefix=f'{prefix}{platform}/')

        if platform == 'illumina':
            paired, single = get_RNASeq_filepaths(filepaths)
            paired_meta = metadata_dict['RNA-Seq']
            single_meta = {**paired_meta, 'library_layout': 'single'}

            for r1, r2 in paired:
                file_metadata[r1] = build_entry(paired_meta, 'fastq', r1, r2)
            for r1 in single:
                file_metadata[r1] = build_entry(single_meta, 'fastq', r1)

        elif platform in ('pacbio_hifi', 'pacbio_kinnex'):
            metadata = metadata_dict['Iso-Seq' if platform == 'pacbio_hifi' else 'Kinnex']
            for filepath in filepaths:
                file = filepath.split('/')[-1]
                if file.split(os.extsep, 1)[-1] == 'bam':
                    file_metadata[file] = build_entry(metadata, 'bam', filepath, assembly='unaligned')

    return file_metadata

def write_file_metadata(file_metadata, filename):
    if not file_metadata:
        return
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    fieldnames = ['biosample_accession', 'library_ID', 'title', 'library_strategy', 'library_source',
                  'library_selection', 'library_layout', 'platform', 'instrument_model',
                  'design_description', 'filetype', 'filename', 'filename2', 'filename3',
                  'filename4', 'assembly', 'fasta_file']
    with open(filename, mode='w', newline='') as out:
        writer = csv.DictWriter(out, delimiter='\t', fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(file_metadata.values())
    print(f'Metadata saved to {filename}')
