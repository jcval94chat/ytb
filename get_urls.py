# get_urls.py
import datetime

def get_urls():
    """
    Retorna la lista de canales a procesar según el día de la semana.
    - Monday=0, Tuesday=1, ..., Sunday=6
    - chunk_size = 40
    """
    channel_urls = [
        "https://www.youtube.com/@MarianoTrejo",
        "https://www.youtube.com/@humphrey",
        "https://www.youtube.com/@MisPropiasFinanzas",
        "https://www.youtube.com/@AdriàSolàPastor",
        "https://www.youtube.com/@EduardoRosas",
        "https://www.youtube.com/@CésarDabiánFinanzas",
        "https://www.youtube.com/@soycristinadayz",
        "https://www.youtube.com/@MorisDieck",
        "https://www.youtube.com/@AdrianSaenz",
        "https://www.youtube.com/@FinanzasparatodosYT",
        "https://www.youtube.com/@LuisMiNegocios",
        "https://www.youtube.com/@AprendizFinanciero",
        "https://www.youtube.com/@negociosyfinanzas2559",
        "https://www.youtube.com/@pequenocerdocapitalista",
        "https://www.youtube.com/@AlexHormozi",
        "https://www.youtube.com/@CalebHammer",
        "https://www.youtube.com/c/Myprimermillón",
        "https://www.youtube.com/@starterstory",
        "https://www.youtube.com/@irenealbacete",
        "https://www.youtube.com/@bulkin_uri",
        "https://www.youtube.com/@ExitoFinancieroOficial",
        "https://www.youtube.com/@compuestospodcast",
        "https://www.youtube.com/@JaimeHigueraEspanol",
        "https://www.youtube.com/@soycristinadayz",
        "https://www.youtube.com/@jefillysh",
        "https://www.youtube.com/@EDteam",
        "https://www.youtube.com/@LatinoSueco",
        "https://www.youtube.com/@Ter",
        "https://www.youtube.com/@doctorfision",
        "https://www.youtube.com/@EvaMariaBeristain",
        "https://www.youtube.com/@Unicoos",
        "https://www.youtube.com/@QuantumFracture",
        "https://www.youtube.com/@lagatadeschrodinger",
        "https://www.youtube.com/@CdeCiencia",
        "https://www.youtube.com/@elrobotdeplaton",
        "https://www.youtube.com/@PhysicsGirl",
        "https://www.youtube.com/@Veritasium",
        "https://www.youtube.com/@Vsauce",
        "https://www.youtube.com/@minutephysics",
        "https://www.youtube.com/@SmarterEveryDay",
        "https://www.youtube.com/@AsapSCIENCE",
        "https://www.youtube.com/@PBSSpaceTime",
        "https://www.youtube.com/@TheActionLab",
        "https://www.youtube.com/@numberphile",
        "https://www.youtube.com/@crashcourse",
        "https://www.youtube.com/@AliAbdaal",
        "https://www.youtube.com/@ThomasFrank",
        "https://www.youtube.com/@MattDAvella",
        "https://www.youtube.com/@NathanielDrew",
        "https://www.youtube.com/@LordDraugr",
        "https://www.youtube.com/@danielfelipemedina",
        "https://www.youtube.com/@omareducacionfinanciera",
        "https://www.youtube.com/@TwoMinutePapers",
        "https://www.youtube.com/@ElConsejeronocturno",
        "https://www.youtube.com/@UnaFisicaSimplificada",
        "https://www.youtube.com/@gustavo-entrala",
        "https://www.youtube.com/@ricardoalcados",
        "https://www.youtube.com/@inversioninteligente_alain",
        "https://www.youtube.com/@MIGALAD",
        "https://www.youtube.com/@paoalmontes",
        "https://www.youtube.com/c/Caf%C3%A9Kyoto",
        "https://www.youtube.com/@watop_esp",
        "https://www.youtube.com/@jefidos",
        "https://www.youtube.com/@judithtiral5713",
        "https://www.youtube.com/@ProgramadorX",
        "https://www.youtube.com/@NocheDeChicxs",
        "https://www.youtube.com/@Platzi",
        "https://www.youtube.com/@DiegoRevueltaTV",
        "https://www.youtube.com/@MIGALAD",
        "https://www.youtube.com/@ricardoalcados",
        "https://www.youtube.com/@lunamartinezoficial",
    ]

    # Cantidad máxima de canales a procesar por día
    chunk_size = 40

    # Obtener el día de la semana (Monday=0, Tuesday=1, ..., Sunday=6)
    weekday = datetime.datetime.today().weekday()

    # Calcular el índice de inicio y fin
    start = weekday * chunk_size
    end = start + chunk_size

    # Seleccionar solo los canales que correspondan a ese día
    daily_channels = channel_urls[start:end]

    return daily_channels
