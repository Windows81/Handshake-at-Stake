# https://github.com/yabjames/handshake_employers_scraper/blob/main/main.py

from typing import Any, final, override
from collections import defaultdict
import requests
import time
import base


REQUEST_HEADERS = {
    'hs-app-platform': 'android',
    'hs-app-version': '4.24.1',
    'hs-app-build': '279',
    'user-agent': 'Handshake Android 4.24.1 (Manufacturer: Microsoft, Model: Surface Duo, OS Version: 12)',
    'accept': 'application/json',
    'accept-language': 'en-GB',
    'authorization': 'Bearer eyJhbGciOiJkaXIiLCJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2Q0JDLUhTNTEyIiwidHlwIjoiSldUIn0..pGumiw8gAQ962EMaiD943g._DiOnUpsiDtF0FlihtREh33eXzLL342GAZG_erMdiReBxGe4LkEtMJQ093cPsLpCCBNF65349U5i6jtMiT_PAfZ4tmfiKfVlune0yOVo-V1vqh3_AHL1gSDgIdGU_KbFXKkeRub0sJVxY3TTk_ZxOKptPfJk8Q7cc1MU23H9FhyqnCkoK-pC6AYdIMSWr5E4NuPZBPjjXzQO688LvYmpmulJJxcQUwx0JSQi2ipKujxIrHrGJXX0CvJlCF5bYtTJPONqHHnjxtGLscOU5LZ_2xyMlqG7QQyFuGD-5B3J-7XFe0g2K2zJRHF665qzYt4C8AzRxp8WzC4kOnvFW5jACkFROxQdG1ZnRY9OT6I3-us7iRXF-tcCY1H0Huxme1ol.V7ECOzDT_fVXcl3MAW8Ae-oWleAJMyF2HfuyyeE3FQA',
}


@final
class data_serialiser:
    IDEN_FIELD = '__id'
    BASE_IDEN_FIELD = '__base_id'

    def __init__(self, base_iden: int, item_list: list[dict[str, Any]]) -> None:
        self.attributes: defaultdict[str, defaultdict[str, list[Any]]] = defaultdict(
            lambda: defaultdict(list)
        )
        self.attribute_names: defaultdict[str, set[str]] = defaultdict(set)
        self.identifiers: list[int] = []
        self.base_iden = base_iden

        self.relationships: set[tuple[int, int, str]] = set()

        for item in item_list:
            type_key: str = item['type']
            self.attribute_names[type_key].update(item['attributes'].keys())

        for item in item_list:
            type_key: str = item['type']
            item_iden: int = int(item['id'])
            self.attributes[type_key][self.IDEN_FIELD].append(item_iden)

            self.relationships.add((
                base_iden,
                item_iden,
                type_key,
            ))

            attributes: dict[str, Any] = item['attributes']
            for field in self.attribute_names[type_key]:
                # You will sometimes get `"None"` as a value.
                value = attributes.get(field, None)
                self.attributes[type_key][field].append(value)

            # The list at `BASE_IDEN_FIELD` contains repeated copies of the base iden.
            self.attributes[type_key][self.BASE_IDEN_FIELD].append(base_iden)

            relationships = item.get('relationships', {})
            for relation_name, related_item in relationships.items():
                related_item_data = related_item['data']
                if not isinstance(related_item_data, dict) or 'id' not in related_item_data:
                    continue
                self.relationships.add((
                    base_iden,
                    int(related_item_data['id']),
                    related_item_data['type'],
                ))

        (
            self.relationship_sources,
            self.relationship_destins,
            self.relationship_destin_types
        ) = (
            zip(*self.relationships)
            if len(self.relationships) else
            ((), (), ())
        )


def refresh_headers() -> None:
    renew_json = requests.get(
        'https://app.joinhandshake.com/mobile/v2/auth/renew_token',
        headers=REQUEST_HEADERS,
    ).json()
    REQUEST_HEADERS['authorization'] = f"Bearer {renew_json['auth_token']}"


@final
class database(base.lambda_database[data_serialiser]):
    INIT_STATEMENTS = ""

    SCHEMA = {
        'CAREER FAIRS': {
            'Identifier': {
                'func': lambda iden, entry: entry.attributes['career-fairs'][entry.BASE_IDEN_FIELD],
                'type': 'integer primary key',
            },
            'Name': {
                'func': lambda iden, entry: entry.attributes['career-fairs']['name'],
                'type': 'text',
            },
            'Logo Url': {
                'func': lambda iden, entry: entry.attributes['career-fairs']['logo-url'],
                'type': 'text',
            },
            'Start Date': {
                'func': lambda iden, entry: entry.attributes['career-fairs']['start-date'],
                'type': 'datetime',
            },
            'End Date': {
                'func': lambda iden, entry: entry.attributes['career-fairs']['end-date'],
                'type': 'datetime',
            },
            'Location Name': {
                'func': lambda iden, entry: entry.attributes['career-fairs']['location-name'],
                'type': 'text',
            },
            'Time Zone': {
                'func': lambda iden, entry: entry.attributes['career-fairs']['time-zone'],
                'type': 'text',
            },
            'Event Check-in Enabled': {
                'func': lambda iden, entry: entry.attributes['career-fairs']['event-checkin-enabled'],
                'type': 'bool',
            },
            'Location Type': {
                'func': lambda iden, entry: entry.attributes['career-fairs']['location-type'],
                'type': 'text',
            },
            'Host Type': {
                'func': lambda iden, entry: entry.attributes['career-fairs']['host-type'],
                'type': 'text',
            },
            'Description': {
                'func': lambda iden, entry: entry.attributes['career-fairs']['student-description'],
                'type': 'text',
            },
            'Student Cost': {
                'func': lambda iden, entry: entry.attributes['career-fairs']['student-cost'],
                'type': 'real',
            },
            'Currency Code': {
                'func': lambda iden, entry: entry.attributes['career-fairs']['currency-iso-code'],
                'type': 'text',
            },
            'Student Limit': {
                'func': lambda iden, entry: entry.attributes['career-fairs']['student-limit'],
                'type': 'integer',
            },
            'Contact Title': {
                'func': lambda iden, entry: entry.attributes['career-fairs']['contact-title'],
                'type': 'integer',
            },
            'Contact Email': {
                'func': lambda iden, entry: entry.attributes['career-fairs']['contact-email'],
                'type': 'integer',
            },
            'Contact Phone': {
                'func': lambda iden, entry: entry.attributes['career-fairs']['contact-phone'],
                'type': 'integer',
            },
        },
        'EMPLOYERS': {
            'Employer Identifier': {
                'func': lambda iden, entry: entry.attributes['employers'][entry.IDEN_FIELD],
                'type': 'integer primary key',
            },
            'Name': {
                'func': lambda iden, entry: entry.attributes['employers']['name'],
                'type': 'text',
            },
            'Description': {
                'func': lambda iden, entry: entry.attributes['employers']['description'],
                'type': 'text',
            },
            'Email': {
                'func': lambda iden, entry: entry.attributes['employers']['email'],
                'type': 'integer',
            },
            'Phone': {
                'func': lambda iden, entry: entry.attributes['employers']['phone'],
                'type': 'integer',
            },
            'Website': {
                'func': lambda iden, entry: entry.attributes['employers']['website'],
                'type': 'integer',
            },
            'Location Name': {
                'func': lambda iden, entry: entry.attributes['employers']['location-name'],
                'type': 'integer',
            },
            'Region': {
                'func': lambda iden, entry: entry.attributes['employers']['region'],
                'type': 'integer',
            },
        },
        'REGISTRATIONS': {
            'Registration Identifier': {
                'func': lambda iden, entry: entry.attributes['registrations'][entry.IDEN_FIELD],
                'type': 'integer primary key',
            },
            'Job Titles': {
                'func': lambda iden, entry: entry.attributes['registrations']['job-titles'],
                'type': 'text',
            },
            'Description': {
                'func': lambda iden, entry: entry.attributes['registrations']['company-description'],
                'type': 'text',
            },
            'Website': {
                'func': lambda iden, entry: entry.attributes['registrations']['website'],
                'type': 'text',
            },
        },
        'RELATIONSHIPS': {
            'Source Identifier': {
                'func': lambda iden, entry: entry.relationship_sources,
                'type': 'integer',
            },
            'Destination Identifier': {
                'func': lambda iden, entry: entry.relationship_destins,
                'type': 'integer',
            },
            'Destination Type': {
                'func': lambda iden, entry: entry.relationship_destin_types,
                'type': 'integer',
            },
        },
    }


@final
class scraper(base.scraper_base[data_serialiser]):
    RANGE_MIN = 52784
    RANGE_MAX = 64157
    DEFAULT_THREAD_COUNT = 3

    @override
    @staticmethod
    def should_print_entry(iden: int, entry) -> bool:
        return entry is not None and len(entry.attributes['career-fairs']) > 0

    @override
    @staticmethod
    def try_entry(iden: int) -> data_serialiser:
        results: list[dict[str, Any]] = []
        career_fair_result = requests.get(
            f'https://app.joinhandshake.com/mobile/v1/career_fairs/{iden}',
            headers=REQUEST_HEADERS,
        ).json()

        if 'errors' not in career_fair_result:
            results.extend(career_fair_result.get('included', []))
            results.append(career_fair_result.get('data', []))

        employer_list_result = requests.get(
            f'https://app.joinhandshake.com/mobile/v2/career_fairs/{iden}/employers_list',
            params={
                'ajax': 'true',
                'category': 'StudentRegistration',
                'including_all_facets_in_searches': 'true',
                'sort_direction': 'asc',
                'sort_column': 'default',
                'qualified_only': '',
                'per_page': '200',
            },
            headers=REQUEST_HEADERS,
        ).json()

        if 'errors' not in employer_list_result:
            results.extend(employer_list_result.get('included', []))
            results.extend(employer_list_result.get('data', []))

        return data_serialiser(iden, results)
