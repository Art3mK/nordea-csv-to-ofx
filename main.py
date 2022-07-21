#!/usr/bin/env python3
import itertools as it
import traceback
import configargparse

from operator import itemgetter
from meza.io import read_csv, IterStringIO, write
from csv2ofx import utils
from csv2ofx.ofx import OFX
from pathlib import Path

def main():
    parser = configargparse.ArgParser(description='Script to convert finnish nordea csv to qfx')
    parser.add_argument('--source', help='path to source csv file, downloaded from nordea netbank', env_var='CSV_TO_OFX_SRC_FILE')
    args = parser.parse_args()

    src = Path(args.source)
    if not src.is_file():
        exit(f"{args.source} doesn't exist (or not a regular file!)")

    mapping = {
        "has_header": True,
        "account": itemgetter("Maksaja"),
        "date": itemgetter("Kirjauspäivä"),
        "amount": itemgetter("Määrä"),
        "desc": itemgetter("Otsikko"),
        "payee": itemgetter("Otsikko"),
        "notes": itemgetter("Notes"),
        "currency": "EUR"
    }

    ofx = OFX(mapping)
    records = read_csv(args.source, has_header=True, delimiter=';')
    groups = ofx.gen_groups(records)
    trxns = ofx.gen_trxns(groups)
    cleaned_trxns = ofx.clean_trxns(trxns)
    data = utils.gen_data(cleaned_trxns)
    content = it.chain([ofx.header(), ofx.gen_body(data), ofx.footer()])

    dest = open(f'{src.cwd()}/{src.stem}.ofx', "w", encoding='utf-8')
    try:
        res = write(dest, IterStringIO(content))
        print(f'Created {dest.name}')
    except KeyError as err:
        msg = "Field %s is missing from file. Check `mapping` option." % err
    except TypeError as err:
        msg = "No data to write. %s. " % str(err)
    except ValueError as err:
        # csv2ofx called with no arguments or broken mapping
        msg = "Possible mapping problem: %s. " % str(err)
    except Exception:  # pylint: disable=broad-except
        msg = 1
        traceback.print_exc()
    else:
        msg = 0 if res else "No data to write. Check `start` and `end` options."
    finally:
        exit(msg)
        dest.close()

if __name__ == '__main__':
    main()
