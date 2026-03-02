
from DataAcquisition import AdvancedGameDataCollector
games = [
    {"name": "Counter-Strike 2", "appid": 730},
    {"name": "Dota 2", "appid": 570},
    {"name": "PUBG: Battlegrounds", "appid": 578080},
    {"name": "Apex Legends", "appid": 1172470},
    {"name": "Grand Theft Auto V", "appid": 271590},
    {"name": "Rust", "appid": 252490},
    {"name": "ARK: Survival Evolved", "appid": 346110},
    {"name": "Destiny 2", "appid": 1085660},
    {"name": "Team Fortress 2", "appid": 440},
    {"name": "Warframe", "appid": 230410},
    {"name": "Rainbow Six Siege", "appid": 359550},
    {"name": "Elden Ring", "appid": 1245620},
    {"name": "Cyberpunk 2077", "appid": 1091500},
    {"name": "Red Dead Redemption 2", "appid": 1174180},
    {"name": "Valheim", "appid": 892970},
    {"name": "Among Us", "appid": 945360},
    {"name": "The Witcher 3: Wild Hunt", "appid": 292030},
    {"name": "Hogwarts Legacy", "appid": 990080},
    {"name": "Battlefield 2042", "appid": 1517290},
    {"name": "Terraria", "appid": 105600},
    {"name": "Monster Hunter: World", "appid": 582010},
    {"name": "Forza Horizon 5", "appid": 1551360},
    {"name": "No Man's Sky", "appid": 275850},
    {"name": "Dead by Daylight", "appid": 381210},
    {"name": "Fallout 4", "appid": 377160},
]
if __name__ == "__main__":
    collector = AdvancedGameDataCollector(games)
    df = collector.run()