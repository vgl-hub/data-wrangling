from generate_SRA_metadata import get_genome_metadata, get_transcriptome_metadata, write_file_metadata
import argparse
import csv
import re

def parse_args():
    parser = argparse.ArgumentParser(
        usage=(
            'Single entry: python SRA_metadata.py [-g | -tx] single -b [BioSample] -t [ToLID] -s [Species_name] [-ts tissue]\n'
            'Batch entry:  python SRA_metadata.py [-g | -tx] batch -f [file.csv] [-ts tissue]'
        )
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-g', '--genome', action='store_true',
                        help='Generate genome sequence metadata')
    group.add_argument('-tx', '--transcriptome', action='store_true',
                        help='Generate transcriptome metadata')

    subparsers = parser.add_subparsers(dest='mode', required=True)

    single_entry = subparsers.add_parser('single')
    single_entry.add_argument('-b', '--biosample', required=True, help='BioSample Accession')
    single_entry.add_argument('-t', '--tolid', required=True, help='ToLID')
    single_entry.add_argument('-s', '--species', required=True,
                              help='Species name in latin (e.g. Homo_sapiens)')
    single_entry.add_argument('-ts', '--tissue',
                              help='Tissue type (required with -tx), e.g. muscle, heart, skin')

    batch_entry = subparsers.add_parser('batch')
    batch_entry.add_argument('-f', '--filename', required=True,
                             help='.csv file containing species information')
    batch_entry.add_argument('-ts', '--tissue',
                             help='Tissue type (required with -tx unless given in the CSV)')

    args = parser.parse_args()

    # Conditional requirement
    if args.transcriptome and args.mode == 'single' and not args.tissue:
        parser.error('-ts/--tissue is required for single transcriptome entries')

    return args

def process_metadata(filename, genomic, transcriptomic):
    species_dict = dict()
    with open(filename, 'r', encoding='utf-8-sig') as file:
        for row in csv.DictReader(file):
            biosample = (row['BioSample'] or '').strip()
            if not biosample:
                continue
            tolid = row['ToLID']
            entry = species_dict.setdefault(
                tolid, {'Species': row['Species'], 'BioSample': None, 'Tissues': {}}
            )
            tissue = (row.get('Tissue') or '').strip().lower().replace(' ', '_')
            if tissue:
                if tissue in entry['Tissues'] and entry['Tissues'][tissue] != biosample:
                    print(f'Warning: conflicting BioSamples for {tolid} {tissue}; using {biosample}.')
                entry['Tissues'][tissue] = biosample
            else:
                entry['BioSample'] = biosample   # row without a tissue = genome BioSample

    for tolid, info in species_dict.items():
        print(info)
        species = info['Species']
        species_name = species.replace(' ', '_')

        if genomic:
            if not info['BioSample']:
                print(f'Warning: no genome BioSample (row with empty Tissue) for {species}, {tolid}; skipping genome metadata.')
            else:
                file_metadata = get_genome_metadata(species, tolid, info['BioSample'])
                out_filename = f'submission_metadata/sequence_metadata/{species_name}_{tolid}_SRA_sequence_metadata.tsv'
                write_file_metadata(file_metadata, out_filename)

        if transcriptomic:
            if not info['Tissues']:
                print(f'Warning: no tissue given for {species}, {tolid}; skipping transcriptome metadata.')
            else:
                file_metadata = dict()
                for tissue, tissue_biosample in info['Tissues'].items():
                    file_metadata.update(
                        get_transcriptome_metadata(species, tolid, tissue_biosample, tissue)
                    )
                out_filename = f'submission_metadata/transcriptome_metadata/{species_name}_{tolid}_SRA_transcriptome_metadata.tsv'
                write_file_metadata(file_metadata, out_filename)

if __name__ == '__main__':
    args = parse_args()
    if hasattr(args, 'biosample') and hasattr(args, 'species') and hasattr(args, 'tolid'):
        if args.genome:
            file_metadata = get_genome_metadata(args.species, args.tolid, args.biosample)
            filename = f'submission_metadata/sequence_metadata/%s_%s_SRA_sequence_metadata.tsv' % (args.species, args.tolid)
            write_file_metadata(file_metadata, filename)
        if args.transcriptome:
            file_metadata = get_transcriptome_metadata(args.species, args.tolid, args.biosample)
            filename = f'submission_metadata/transcriptome_metadata/%s_%s_SRA_transcriptome_metadata.tsv' % (args.species, args.tolid)
            write_file_metadata(file_metadata, filename)
    elif args.filename:
        process_metadata(args.filename, genomic = args.genome, transcriptomic = args.transcriptome)