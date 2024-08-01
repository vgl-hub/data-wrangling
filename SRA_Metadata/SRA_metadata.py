from generate_SRA_metadata import get_genome_metadata, get_transcriptome_metadata, write_file_metadata
import argparse
import csv

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-g', '--genome', action = 'store_true', help = 'Generate genome sequence metadata'
    )
    parser.add_argument(
        '-tx', '--transcriptome', action = 'store_true', help = 'Generate transcriptome metadata'
    )
    subparsers = parser.add_subparsers()
    single_entry = subparsers.add_parser('single')
    single_entry.add_argument(
        '-b', '--biosample', help = 'BioSample Accession', required = True
    )
    single_entry.add_argument(
        '-t', '--tolid', help = 'ToLID', required = True
    )
    single_entry.add_argument(
        '-s', '--species', help = 'Species name in latin (e.g. Homo_sapiens)', required = True
    )
    batch_entry = subparsers.add_parser('batch')
    batch_entry.add_argument(
        '-f', '--filename', help = '.csv file containing species information', required = True
    )
    return parser.parse_args()

def process_metadata(filename, genomic, transcriptomic):
    species_dict = dict()
    with open(filename, 'r', encoding='utf-8-sig') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            if row['BioSample']:
                species_dict[row['ToLID']] = {'BioSample':row['BioSample'], 'Species':row['Species']}
    for tolid in species_dict.keys():
        print(species_dict[tolid])
        species= species_dict[tolid]['Species']
        biosample_accession = species_dict[tolid]['BioSample']
        if genomic:
            file_metadata = get_genome_metadata(species, tolid, biosample_accession)
            filename = f'submission_metadata/sequence_metadata/%s_%s_SRA_sequence_metadata.tsv' % (species.replace(' ', '_'), tolid)
            if file_metadata:
                write_file_metadata(file_metadata, filename)
        if transcriptomic:
            file_metadata = get_transcriptome_metadata(species, tolid, biosample_accession)
            filename = f'submission_metadata/transcriptome_metadata/%s_%s_SRA_transcriptome_metadata.tsv' % (species.replace(' ', '_'), tolid)
            if file_metadata:
                write_file_metadata(file_metadata, filename)

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