# INDICES

1. [Requerimientos](#requerimientos)
2. [Correr el proyecto por primera vez](#correr-el-proyecto-por-primera-vez)

## Requerimientos

Necesitas los siguientes programas para poder correr el proyecto.

- [Python](https://www.python.org)
- [MySQL](https://www.mysql.com)

## Correr el proyecto por primera vez

### Windows

- Crea una base de datos con el nombre que desees en MySQL para ser utilizada por el proyecto con el comando.

        CREATE SCHEMA nombre_base_datos

- Asegurate de tener [python](https://www.python.org/) instalado en tu máquina y continua con los siguientes pasos.

- Abre una terminal de powershell o cmd dentro de la carpeta "backend".

- Crea un entorno virtual **(venv)** para python.

        python -m venv venv # windows
        python3 -m venv venv # linux o macos

- Activa el venv utilizando el siguiente comando

        venv\Scripts\activate # windows
        source venv/bin/activate # linux o macos

    Si obtienes un error como el siguiente en Windows

        venv\Scripts\activate : The module 'venv' could not be loaded. For more information, run 'Import-Module venv'...

    Corre este comando primero y después vuelve a intentar correr el primer comando

        Set-ExecutionPolicy Unrestricted -Scope Process

- Para saber si el venv se activó, verifica que tu línea en terminal comienza con `(venv)`.

- Ahora debes de descargar los requerimientos del proyecto con el comando

        pip install -r requirements.txt

- Genera una llave secreta para el proyecto en la siguiente página **[djecrety](https://djecrety.ir/)**, la necesitas para el siguiente paso.

- Una vez que se hayan instalado correctamente, crea un archivo en la raíz del directorio "backend" que se llame ".env" siguiendo el ejemplo del archivo ".env.example" y llenalo con tus datos.

- Corre las migraciones de django para la base de datos realizando los dos siguientes comandos

        python manage.py makemigrations
        python manage.py migrate

- Corre los fixtures para cargar los datos de carreras y planes

    python manage.py loaddata carreras
    python manage.py loaddata planes

- Crea un superusuario para acceder a todos los permisos de la aplicación

        python manage.py createsuperuser

    Te pedirá unos datos para crearlo, realmente no importa que ingreses pero recuerda el nombre de usuario, email y contraseña para acceder a la aplicación.

- Finalmente corre el backend con el comando

        python manage.py runserver

    Verás un mensaje de que el servidor está corriendo en servidor local [localhost:8000](http://localhost:8000). Cuando necesites detener el servidor utiliza `CTRL+C` dentro de la terminal, por el momento dejalo corriendo.
    Podras acceder a la interfaz de la API dentro de las rutas en [localhost:8000](http://localhost:8000).
