"""Taller evaluable"""

# pylint: disable=broad-exception-raised

import fileinput
import glob
import os.path
import time
from itertools import groupby

from toolz.itertoolz import concat, pluck



def copy_raw_files_to_input_folder(n):
    """Generate n copies of the raw files in the input folder"""
    origen = "files/raw"
    destino = "files/input"

    if not os.path.exists(destino):
        os.makedirs(destino)
    
    archivos = glob.glob(os.path.join(origen, "*"))

    for archivo in archivos:
        nombre = os.path.basename(archivo)
        nombre_base, extension = os.path.splitext(nombre)

        for i in range(1, n + 1):
            nuevo_nombre = f"{nombre_base}_copy{i}{extension}"
            nueva_ruta = os.path.join(destino, nuevo_nombre)

            with open(archivo, "rb") as src, open(nueva_ruta, "wb") as dst:
                for chunk in iter(lambda: src.read(4096), b""):
                    dst.write(chunk)

            print(f"Generada copia: {nueva_ruta}")

def load_input(input_directory):
    """Funcion load_input"""
    archivos = glob.glob(os.path.join(input_directory, "*"))
    datos = []

    for line in fileinput.input(files=archivos):
        datos.append((os.path.basename(fileinput.filename()), line.strip()))

    return datos

def preprocess_line(x):
    """Preprocess the line x"""
    x = x.lower()
    resultado = []
    prev_was_space = False

    for char in x:
        if char.isalnum():
            resultado.append(char)
            prev_was_space = False
        else:
            if not prev_was_space:
                resultado.append(" ")
                prev_was_space = True

    return "".join(resultado).strip()

def map_line(x):
    pass

def mapper(sequence):
    """Mapper"""
    pares = []
    for _, linea in sequence:
        for palabra in linea.split():
            pares.append((palabra, 1))
    return pares

def shuffle_and_sort(sequence):
    """Shuffle and Sort"""
    sequence.sort(key=lambda x: x[0])

    resultado = []
    for clave, grupo in groupby(sequence, key=lambda x: x[0]):
        valores = list(pluck(1, grupo))
        resultado.append((clave, valores))

    return resultado


def compute_sum_by_group(group):
    pass

def reducer(sequence):
    """Reducer"""
    resultado = []

    for clave, valores in sequence:
        resultado.append((clave, sum(valores)))

    return resultado

def create_directory(directory):
    """Create Output Directory"""
    if not os.path.exists(directory):
        os.makedirs(directory)


def save_output(output_directory, sequence):
    """Save Output"""
    output_file = os.path.join(output_directory, "part-00000")
    
    create_directory(output_directory)

    with open(output_file, "w", encoding="utf-8") as f:
        for clave, valor in sequence:
            f.write(f"{clave}\t{valor}\n")
    
    print(f"Resultado guardado en: {output_file}")

def create_marker(output_directory):
    """Create Marker"""
    marker_file = os.path.join(output_directory, "_SUCCESS")
    with open(marker_file, "w", encoding="utf-8") as f:
        f.write(f"Proceso terminado correctamente\n")
        f.write(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    print(f"Marker creado en: {marker_file}")

def run_job(input_directory, output_directory):
    """Job"""
    sequence = load_input(input_directory)

    sequence = [(archivo, preprocess_line(linea)) for archivo, linea in sequence]

    sequence = mapper(sequence)
    sequence = shuffle_and_sort(sequence)
    sequence = reducer(sequence)
    create_directory(output_directory)
    save_output(output_directory, sequence)
    create_marker(output_directory)


if __name__ == "__main__":

    copy_raw_files_to_input_folder(n=2)

    start_time = time.time()

    run_job(
        "files/input",
        "files/output",
    )

    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")