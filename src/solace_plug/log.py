import logging

log = logging.getLogger("solace_plug")
# log.addHandler(logging.NullHandler())

#TODO: Remove this once we are done testing
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)