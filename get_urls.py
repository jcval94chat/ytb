# get_urls.py
from __future__ import annotations

import datetime as dt
import math
import os
from typing import Any

CHANNEL_URLS = [
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
    "https://www.youtube.com/@lunamartinezoficial",
    "https://www.youtube.com/@semo_historia",
    "https://www.youtube.com/@NateGentile7",
    "https://www.youtube.com/@DEMENTESMedia",
    "https://www.youtube.com/@FrancoPisso",
    "https://www.youtube.com/@witchcalli",
    "https://www.youtube.com/@viyaura",
    "https://www.youtube.com/@ArcadesBooks",
    "https://www.youtube.com/@DonebyLaura",
    "https://www.youtube.com/@TwoCentsPBS",
    "https://www.youtube.com/@bravosresearch",
    "https://www.youtube.com/@ThatNateBlack",
    "https://www.youtube.com/@DiegoRuzzarin",
    "https://www.youtube.com/@ManCarryingThing",
    "https://www.youtube.com/@comofuncionatunegocio",
    "https://www.youtube.com/@irenesauriox",
    "https://www.youtube.com/@98granos",
    "https://www.youtube.com/@WoshingoStreams",
    "https://www.youtube.com/@midulive",
    "https://www.youtube.com/@AleAbsolutable",
    "https://www.youtube.com/@RedditHistoriasSinFiltro",
    "https://www.youtube.com/@SantiagoAmatFinanzas",
    "https://www.youtube.com/@Brinnn",
    "https://www.youtube.com/@DrossRotzank",
    "https://www.youtube.com/@RicardoAlcaraz",
    "https://www.youtube.com/@RobPaperClips",
    "https://www.youtube.com/@RobPaperSheet",
    "https://www.youtube.com/@Pinwinsillo",
    "https://www.youtube.com/@atherion",
    "https://www.youtube.com/@ProfePiola",
    "https://www.youtube.com/@Sapienciapráctica",
    "https://www.youtube.com/@ImpulsoSupremo333",
    "https://www.youtube.com/@noctivoyt",
    "https://www.youtube.com/@Unpocomejor1",
    "https://www.youtube.com/@elWacky2",
    "https://www.youtube.com/@Sambucha",
    "https://www.youtube.com/@thefinancialfreedomgirl",
    "https://www.youtube.com/@ALXI_ESSAY",
    "https://www.youtube.com/@Landerdiazdelfresno",
    "https://www.youtube.com/@soydalto",
    "https://www.youtube.com/@ELCACHONDO",
    "https://www.youtube.com/@bravogabobravo",
    "https://www.youtube.com/@Eze.martinez",
    "https://www.youtube.com/@vidaprogramador",
    "https://www.youtube.com/@DotDager",
    "https://www.youtube.com/@elrincondeldev",
    "https://www.youtube.com/@mourede",
    "https://www.youtube.com/@FaztTech",
    "https://www.youtube.com/@bilinkis",
    "https://www.youtube.com/@ELMAIKINIDEV",
    "https://www.youtube.com/@IA_Innova",
    "https://www.youtube.com/@WesRoth",
    "https://www.youtube.com/@RingaTech",
    "https://www.youtube.com/@alejavirivera",
    "https://www.youtube.com/@FranqitoM",
    "https://www.youtube.com/@MisterKrax",
    "https://www.youtube.com/@vidamrr",
    "https://www.youtube.com/@EmprendeAprendiendo",
    "https://www.youtube.com/@KellyStamps",
    "https://www.youtube.com/@PlanetaJuan",
    "https://www.youtube.com/@CorduraArtificial",
    "https://www.youtube.com/@EsquizofreniaNatural",
    "https://www.youtube.com/@Monitorfantasma",
    "https://www.youtube.com/@dateunvlog",
    "https://www.youtube.com/@clem/videos",
    "https://www.youtube.com/@betterideas",
    "https://www.youtube.com/@jomakaze",
    "https://www.youtube.com/@KenJee_ds",
    "https://www.youtube.com/@HolaMundoDev",
    "https://www.youtube.com/@CreaYTransforma",
    "https://www.youtube.com/@TinaHuang1",
    "https://www.youtube.com/@ClaraCarmona",
    "https://www.youtube.com/@jessicafernandezgarcia",
    "https://www.youtube.com/@DonCanitro",
    "https://www.youtube.com/@AI_In_Context",
    "https://www.youtube.com/@TheDiaryOfACEO",
    "https://www.youtube.com/@GothamChess",
]

DEFAULT_CHUNK_SIZE = 40


def _resolve_reference_date() -> dt.date:
    """
    Devuelve la fecha de referencia usada para calcular el bloque semanal.

    - Si existe REFERENCE_DATE en formato YYYY-MM-DD, se usa esa fecha.
    - Si no existe, se usa la fecha actual en UTC.
    """
    reference_date = os.environ.get("REFERENCE_DATE", "").strip()

    if not reference_date:
        return dt.datetime.now(dt.UTC).date()

    try:
        return dt.date.fromisoformat(reference_date)
    except ValueError as exc:
        raise ValueError(
            "REFERENCE_DATE debe tener el formato YYYY-MM-DD. "
            f"Valor recibido: {reference_date!r}"
        ) from exc


def _resolve_chunk_index(total_chunks: int, iso_week: int) -> int:
    """
    Calcula qué bloque toca procesar.

    - Si FORCE_CHUNK_INDEX está informado, lo usa para forzar un bloque concreto.
    - Si no, rota automáticamente usando la semana ISO actual.
    """
    forced_chunk = os.environ.get("FORCE_CHUNK_INDEX", "").strip()

    if forced_chunk:
        try:
            chunk_index = int(forced_chunk)
        except ValueError as exc:
            raise ValueError(
                "FORCE_CHUNK_INDEX debe ser un entero entre 0 y "
                f"{total_chunks - 1}. Valor recibido: {forced_chunk!r}"
            ) from exc

        if not 0 <= chunk_index < total_chunks:
            raise ValueError(
                "FORCE_CHUNK_INDEX está fuera de rango. Debe estar entre 0 y "
                f"{total_chunks - 1}. Valor recibido: {chunk_index}"
            )

        return chunk_index

    return (iso_week - 1) % total_chunks


def get_weekly_chunk_info(chunk_size: int = DEFAULT_CHUNK_SIZE) -> dict[str, Any]:
    """
    Devuelve la información del bloque semanal a procesar.

    La rotación es automática por semana ISO, de modo que una ejecución semanal
    avance por los canales sin necesidad de editar el repositorio manualmente.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size debe ser mayor que 0")

    reference_date = _resolve_reference_date()
    iso_week = reference_date.isocalendar().week
    total_channels = len(CHANNEL_URLS)
    total_chunks = max(1, math.ceil(total_channels / chunk_size))
    chunk_index = _resolve_chunk_index(total_chunks, iso_week)

    start = chunk_index * chunk_size
    end = min(start + chunk_size, total_channels)
    channel_urls = CHANNEL_URLS[start:end]

    return {
        "reference_date": reference_date.isoformat(),
        "iso_week": iso_week,
        "chunk_index": chunk_index,
        "chunk_number": chunk_index + 1,
        "total_chunks": total_chunks,
        "chunk_size": chunk_size,
        "total_channels": total_channels,
        "start_index": start,
        "end_index": end,
        "channel_urls": channel_urls,
    }


def get_urls() -> list[str]:
    """Retorna únicamente las URLs del bloque semanal que toca ejecutar."""
    return get_weekly_chunk_info()["channel_urls"]
