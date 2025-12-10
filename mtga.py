import glob
import os
import io
import sqlite3
import UnityPy
import cv2
import numpy as np


class mtga_reader:
        mtga_root_dir = None
        mtga_data_dir = None
        mtga_assets_dir = None
        mtga_raw_dir = None
        lang = None
        lang_table = None
        default_lang_table = None
        connections = {}
        enums = {}

        def __init__(self, mtga_root_dir, lang='en'):
                self.lang = lang
                self.mtga_root_dir = mtga_root_dir
                self.mtga_data_dir = os.path.join(self.mtga_root_dir, "MTGA_Data")
                self.mtga_assets_dir = os.path.join(self.mtga_data_dir, "Downloads", "AssetBundle")
                self.mtga_raw_dir = os.path.join(self.mtga_data_dir, "Downloads", "Raw")
                self.get_databases()
                self.set_language(lang)
                self.get_enums()

	def dict_factory(self, cursor, row):
		d = {}
		for idx, col in enumerate(cursor.description):
			d[col[0]] = row[idx]
		return d

        def get_databases(self):
                try:
                        dbs = ['ArtCropDatabase', 'CardDatabase', 'ClientLocalization', 'altArtCredits', 'altFlavorTexts', 'credits']
                        for db in dbs:
                                self.connections[db] = sqlite3.connect(
                                        max(
                                                glob.glob(os.path.join(self.mtga_raw_dir, f"Raw_{db}_*.mtga")),
                                                key=os.path.getctime
                                        )
                                )
                                self.connections[db].row_factory = self.dict_factory
                        return True
                except Exception:
                        self.connections = {}
                        return False

        def set_language(self, lang):
                """Validate and store the localization table for the chosen language."""
                cursor = self.connections['CardDatabase'].cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'Localizations_%'")
                available_tables = [row['name'] for row in cursor.fetchall()]

                def normalize(name):
                        return name.replace('-', '').replace('_', '').lower()

                available_langs = {normalize(name.split('Localizations_')[1]): name for name in available_tables}

                if not available_langs:
                        raise ValueError("No localization tables found in CardDatabase.")

                normalized_target = normalize(lang)
                matched_table = available_langs.get(normalized_target)

                self.default_lang_table = available_langs.get(normalize('enUS')) or next(iter(available_tables))

                if not matched_table:
                        raise ValueError(
                                f"Language '{lang}' not available. Options: {', '.join(sorted(available_langs.keys()))}"
                        )

                self.lang = lang
                self.lang_table = matched_table
                return matched_table

        def close(self):
                for db in self.connections:
                        self.connections[db].close()
                return True

        def get_enums(self):
                cursor = self.connections['CardDatabase'].cursor()
                cursor.execute('Select "Type" FROM Enums GROUP BY "Type"')

                for linha in cursor.fetchall():
                        self.enums[linha['Type']] = {}

                for enum_type in self.enums:
                        cursor.execute('Select Value, LocId  FROM Enums WHERE "Type" = ?', (enum_type,))
                        for linha in cursor.fetchall():
                                self.enums[enum_type][linha['Value']] = self.get_card_translation_id(linha['LocId'])

                return True

        def _lookup_localization(self, text_id, table_name):
                cursor = self.connections['CardDatabase'].cursor()
                cursor.execute(
                        f'SELECT Loc FROM {table_name} WHERE LocId = ? ORDER BY Formatted DESC LIMIT 1', (text_id,)
                )
                row = cursor.fetchone()
                return row['Loc'] if row else None

        def get_card_translation_id(self, text_id):
                if text_id is None:
                        return None

                try:
                        translation = self._lookup_localization(text_id, self.lang_table)
                        if translation:
                                return translation

                        if self.default_lang_table and self.default_lang_table != self.lang_table:
                                fallback = self._lookup_localization(text_id, self.default_lang_table)
                                if fallback:
                                        return fallback
                except Exception:
                        pass

                return text_id

        def get_card_abilities(self, ability_id):
                try:
                        cursor = self.connections['CardDatabase'].cursor()
                        cursor.execute('select * from Abilities WHERE Id = ?', (ability_id,))
                        ret = []
                        for linha in cursor.fetchall():
                                linha['TextId'] = self.get_card_translation_id(linha['TextId'])
                                ret.append(linha)
                        return ret[0] if ret else None
                except Exception:
                        return ability_id

        def get_card_by_id(self, card_id, get_art=True):
                cursor = self.connections['CardDatabase'].cursor()
                cursor.execute('SELECT * FROM Cards WHERE GrpId = ? LIMIT 1', (card_id,))
                ret = []
                for linha in cursor.fetchall():
                        tmp = {}
                        for key, val in linha.items():
                                if 'TextId' in key or 'TitleId' in key:
                                        tmp[key.replace("Id", "").lower()] = val if val is None else self.get_card_translation_id(val)
                                elif 'AbilityIds' in key:
                                        tmp[key.replace("Id", "").lower()] = val if val is None else self.get_card_abilities(val)
                                elif 'ArtId' in key:
                                        tmp['art'] = val if (val is None or not get_art) else self.get_card_art_by_id(val)
                                else:
                                        tmp[key] = val
                        ret.append(tmp)
                return ret[0] if ret else None

        def get_card_by_name(self, card_name, limit=None, get_art=True):
                cursor = self.connections['CardDatabase'].cursor()

                query = (
                        f'SELECT GrpId FROM Cards WHERE TitleId IN (select LocId from {self.lang_table} WHERE Loc like ?)'
                        + (f' LIMIT ?' if limit else '')
                )
                params = [card_name]
                if limit:
                        params.append(limit)

                cursor.execute(query, params)
                ret = []
                for linha in cursor.fetchall():
                        ret.append(self.get_card_by_id(linha['GrpId'], get_art))
                return ret

	def find_card_art_file(self, card_id):
		ret = {
			'image': None,
			'util': None
		}

		for file_name in glob.glob(f"{self.mtga_assets_dir}{str(card_id).zfill(6)}*.mtga"):
			env = UnityPy.load(file_name)

			for path, obj in env.container.items():
				if obj.type.name in ["Texture2D", "Sprite"]:
					data = obj.read()
					img_byte_arr = io.BytesIO()
					data.image.save(img_byte_arr, format='PNG')

					image = np.asarray(bytearray(img_byte_arr.getvalue()), dtype="uint8")
					image = cv2.imdecode(image, cv2.IMREAD_COLOR)

					if 'Util' in path:
						ret['util'] = image
					if f'{card_id}_AIF.' in path:
						ret['image'] = image
					else:
						tmp = path.split(".")[0].split("_")[-1]
						ret[tmp] = image
		return ret

	def get_card_art_by_id(self, card_id):
		tmp = self.find_card_art_file(card_id)
		return tmp
