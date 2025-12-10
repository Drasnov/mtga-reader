from mtga import *

mtga = mtga_reader(os.path.join("E:\\", "MTGA"), lang='enUS')
card = mtga.get_card_by_name("captain sisay", get_art=False)[0]
print(card)
art = mtga.get_card_art_by_id(card['art'])
print(art)
