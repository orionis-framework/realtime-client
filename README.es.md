# Cliente JavaScript de Orionis Realtime

Cliente ESM sin dependencias para **Orionis Realtime v1 sobre WebSocket JSON**. Gestiona el handshake de disponibilidad, llamadas correlacionadas, eventos y callbacks del servidor, streams asíncronos, cancelación, buffers limitados, heartbeat y reconexión del transporte.

Se conecta a endpoints `Route.hub(..., protocol="json")`. No implementa protocolos propios de WebSocket, SSE, MessagePack ni compatibilidad con SignalR.

[English documentation](./README.md) · [Guías del framework](https://docs.orionis-framework.com/es/asynchronous/realtime)

## Instalación

### npm

```bash
npm install orionis-realtime-client
```

```javascript
import { RealtimeClient } from "orionis-realtime-client";
```

El paquete utiliza únicamente ESM, incluye declaraciones de TypeScript y no tiene dependencias de ejecución ni paso de compilación. Utiliza un navegador moderno o Node.js 22 o posterior con soporte nativo de `WebSocket`, `AbortController` y `TextEncoder`. Importar el módulo no crea conexiones.

### Importación directa en el navegador

Copia `node_modules/orionis-realtime-client/orionis-realtime.js` al directorio de assets que sirva tu aplicación e impórtalo desde otro módulo o `<script type="module">`. Ajusta la URL de importación a esa ubicación. El navegador debe cargar los archivos mediante HTTP(S).

Los siguientes ejemplos para navegador colocan ambos módulos JavaScript en el mismo directorio:

```javascript
import { RealtimeClient } from "./orionis-realtime.js";
```

## Inicio rápido con el chat documentado

Configura primero el [Hub y la ruta del chat](https://docs.orionis-framework.com/es/asynchronous/realtime/chat). Ese ejemplo proporciona `/realtime/chat/{room:slug}`, el método `publish` y el evento `message`; el `onConnect()` del servidor une la conexión a la sala de su URL.

Este módulo de navegador se conecta a `lobby`, muestra mensajes recibidos, publica uno y cierra el cliente:

```javascript
import { RealtimeClient, RemoteError } from "./orionis-realtime.js";

const url = new URL("/realtime/chat/lobby", location.href);
url.protocol = location.protocol === "https:" ? "wss:" : "ws:";

const client = new RealtimeClient(url);
const removeMessage = client.on("message", ([message]) => {
    console.log("Chat message:", message);
});
const removeState = client.onState(({ state, previous, attempt }) => {
    console.log(previous, "→", state, "attempt", attempt);
});
const removeError = client.onError((error) => {
    console.error(error.code, error.message);
});

try {
    await client.connect();
    console.log("Connection:", client.connectionId);

    const delivery = await client.invoke(
        "publish",
        [{ author: "Ada", text: "Hello from the downloadable client" }],
        { timeoutMs: 10000, serverTimeout: 5 },
    );
    console.log("Delivery acknowledgement:", delivery);
} catch (error) {
    if (error instanceof RemoteError) {
        console.error("The Hub rejected the call:", error.code, error.message);
    } else {
        console.error(error);
    }
} finally {
    removeMessage();
    removeState();
    removeError();
    await client.dispose();
}
```

Mantén el cliente conectado en una página interactiva y libéralo cuando se retire su componente. Un acuse no garantiza persistencia ni visualización. Un timeout o una desconexión no demuestra que la operación remota no se ejecutara; no reenvíes mutaciones automáticamente.

## API pública

| API | Comportamiento |
| --- | --- |
| `new RealtimeClient(url, options?)` | Acepta un string de URL, un objeto `URL` o una fábrica asíncrona de URL. |
| `connect(): Promise<void>` | Idempotente; resuelve después de un mensaje Realtime `ready` válido, no sólo al abrir WebSocket. |
| `stop(): Promise<void>` | Detiene transporte y reconexión; `connect()` puede iniciarlo nuevamente. |
| `dispose(): Promise<void>` | Cierre definitivo. Esta instancia no puede volver a conectarse. |
| `state` | `idle`, `connecting`, `connected`, `reconnecting`, `stopping` o `disposed`. |
| `connectionId` | ID del servidor mientras está conectado; `null` en otros estados. |
| `invoke(target, args?, options?)` | Devuelve una promesa con el valor JSON del resultado. Requiere conexión activa. |
| `stream(target, args?, options?)` | Inicia inmediatamente un stream y devuelve su iterador asíncrono. |
| `on(target, handler)` | Recibe un **array** de argumentos del evento; devuelve una función para cancelar la suscripción. |
| `handle(target, handler)` | Registra un handler invocable por el servidor por nombre; devuelve una función para retirarlo. Rechaza duplicados. |
| `onState(handler)` | Recibe `{ state, previous, attempt }`; devuelve una función para cancelar la suscripción. |
| `onError(handler)` | Observa instancias normalizadas de `RealtimeError`; devuelve una función para cancelar la suscripción. |

`invoke` y `stream` reciben `args` como array y las opciones `{ kwargs, timeoutMs, serverTimeout, signal }`. `kwargs` es un objeto JSON. El nombre del destino no puede estar vacío, superar 128 puntos de código Unicode ni comenzar con `_`; además debe corresponder a una operación o evento de tu Hub.

`timeoutMs` es un plazo local total en **milisegundos**, con el valor de `invocationTimeoutMs` por defecto; `0` lo desactiva. `serverTimeout` es opcional, se envía en el campo `timeout` del protocolo en **segundos** y debe cumplir la política del servidor. Si lo omites, esa política decide el plazo. Son dos límites independientes.

Las entradas inválidas producen `TypeError`. Una invocación rechaza su promesa; preparar un stream puede lanzar inmediatamente. `onError` permite observar fallos de transporte, protocolo y callbacks; no reemplaza la captura del error de cada operación.

En el navegador, las URL relativas se resuelven respecto a la página actual. Las URL HTTP(S) se convierten a WS(S). Se rechazan credenciales en la URL y fragmentos. Los fallos de conexión o de la fábrica de URL rechazan `connect()` con `ConnectionError` y conservan el error original como causa.

## Consumir un stream

Prepara el [ejemplo FeedHub](https://docs.orionis-framework.com/es/asynchronous/realtime/protocol-and-streaming), que publica `/realtime/feed` y `updates(count)`:

```javascript
import { RealtimeClient } from "./orionis-realtime.js";

const url = new URL("/realtime/feed", location.href);
url.protocol = location.protocol === "https:" ? "wss:" : "ws:";
const client = new RealtimeClient(url);

try {
    await client.connect();

    const updates = client.stream("updates", [3], {
        timeoutMs: 15000,
        serverTimeout: 10,
    });
    for await (const value of updates) {
        console.log("Update:", value); // 0, 1, 2.
    }
} finally {
    await client.dispose();
}
```

Un stream se inicia **al crearlo**: registra y envía la invocación antes del primer `next()`. Consúmelo o cancélalo. Sólo admite un `next()` pendiente; no realiza lecturas concurrentes. Un `break` en `for await` llama a `return()` y cancela la invocación remota. `stream.cancel(reason)` también cancela; el motivo se conserva localmente como causa del error.

Los buffers tienen límites de elementos y bytes. Un consumidor lento puede recibir `LimitError`; el cliente no crea una cola ilimitada ni solicita reproducir el stream al reconectarse.

Para cancelar una llamada normal mientras hay conexión:

```javascript
const controller = new AbortController();
const pending = client.invoke(
    "publish",
    [{ author: "Ada", text: "A cancellable operation" }],
    { signal: controller.signal, timeoutMs: 10000 },
);
controller.abort("The user cancelled");

try {
    await pending;
} catch (error) {
    console.log(error.code); // aborted.
}
```

Solicitar cancelación no revierte efectos ya completados en el servidor.

## Atender una llamada del servidor

El [ejemplo ConfirmationHub](https://docs.orionis-framework.com/es/asynchronous/realtime/protocol-and-streaming) invoca `confirmAction` en un cliente. Registra el handler antes de conectar:

```javascript
const removeConfirmation = client.handle(
    "confirmAction",
    ([payload], { signal, connectionId }) => {
        console.log("Server call on:", connectionId);
        if (signal.aborted) return false;
        return window.confirm(String(payload.question));
    },
);
```

Los handlers reciben un array de argumentos y `{ signal, connectionId }`. Pueden devolver un valor JSON o una promesa; `undefined` se convierte en `null`. Un handler asíncrono debe respetar `signal` cuando realice trabajo cancelable. Sustituye el diálogo sencillo por la interfaz de tu aplicación cuando corresponda. Retíralo con `removeConfirmation()`.

Los handlers de eventos registrados con `on` no devuelven resultados. Eventos y llamadas del servidor comparten el presupuesto global `maxConcurrentHandlers` entre conexiones y están limitados por `callbackTimeoutMs`. El plazo aborta el callback y, para llamadas del servidor, envía una respuesta de error sin esperar indefinidamente. Una promesa de aplicación que ignore la cancelación retiene su plaza hasta terminar realmente. JavaScript no puede detener ese trabajo por la fuerza.

Se pueden registrar como máximo 1024 suscripciones combinadas de eventos, métodos, estados y errores. Cancela las suscripciones al retirar un componente para liberar su presupuesto de registros.

Los observadores `onState` y `onError` que devuelven promesas admiten una llamada pendiente por función registrada y como máximo 1024 promesas de observadores sin finalizar por cliente. Se omiten notificaciones mientras esa función siga pendiente o el presupuesto global esté lleno, en lugar de encolarlas. Cancelar la suscripción no libera la plaza de una promesa antes de que termine. Mantén breves estos observadores; los fallos de observadores de error se contienen.

## Opciones de conexión y valores predeterminados

Las opciones de tiempo de esta tabla se expresan en milisegundos:

| Opción | Valor predeterminado |
| --- | --- |
| `protocols` | `[]`; subprotocolos WebSocket, no selección de codec. |
| `webSocketFactory` | `WebSocket` nativo; función síncrona `(url, protocols) => socket`. |
| `connectTimeoutMs` | `10000` |
| `closeTimeoutMs` | `1000` |
| `invocationTimeoutMs` | `30000`; `0` desactiva el plazo local. |
| `callbackTimeoutMs` | `30000` |
| `heartbeatIntervalMs` | `30000`; `0` desactiva heartbeat. |
| `heartbeatTimeoutMs` | `10000` |
| `sendTimeoutMs` | `10000` |
| `maxPendingInvocations` | `128` |
| `maxConcurrentHandlers` | `32` |
| `maxMessageBytes` | `1048576` |
| `maxQueuedBytes` | `2097152` |
| `maxBufferedAmount` | `262144` |
| `maxStreamBufferItems` | `64` |
| `maxStreamBufferBytes` | `1048576` |
| `maxJsonDepth` | `64` |
| `maxJsonNodes` | `100000` |
| `reconnect.enabled` | `true` |
| `reconnect.maxAttempts` | `10` |
| `reconnect.initialDelayMs` | `500` |
| `reconnect.maxDelayMs` | `30000` |
| `reconnect.factor` | `2` |
| `reconnect.jitter` | `0.2` |

La fábrica de URL recibe `{ attempt, signal }`: la conexión inicial es el intento `0` y la primera reconexión es `1`. Puede devolver una URL asíncronamente para que tu aplicación resuelva datos de conexión actualizados. `webSocketFactory` es independiente y síncrona; permite transportes compatibles en pruebas u otros entornos.

Las fábricas de URL tienen además un límite fijo de 32 callbacks realmente pendientes por cliente, entre generaciones de conexión. Una fábrica que ignore la cancelación retiene su plaza hasta finalizar. Al alcanzar el límite, otro intento falla con `connect_failed` y una causa `LimitError` sin llamar a la fábrica; siguen aplicándose los límites de reconexión configurados.

Los presupuestos numéricos y plazos en milisegundos son enteros positivos seguros, excepto `invocationTimeoutMs`, `heartbeatIntervalMs` y `maxBufferedAmount`, que admiten cero. Los temporizadores no pueden superar `2147483647` milisegundos. Los intentos y la espera inicial de reconexión admiten cero, `factor` es como mínimo `1` y `jitter` va de `0` a `1`; la espera máxima debe ser igual o mayor que la inicial. Se admiten hasta 64 subprotocolos únicos y válidos de WebSocket. Las invocaciones aceptan hasta 64 argumentos posicionales y 64 nombrados. Se rechazan nombres de opciones desconocidos.

La reconexión restaura **únicamente el transporte**. Las llamadas y los streams pendientes fallan al desconectarse. No existe una cola de invocaciones sin conexión, reenvío automático, repetición de membresías ni historial de eventos. La nueva conexión ejecuta de nuevo el ciclo del Hub en el servidor; restaura explícitamente el estado de la aplicación cuando corresponda.

La reconexión automática se limita a los códigos de cierre `1001`, `1006`, `1011`, `1012`, `1013` y `1014`, dentro del presupuesto de intentos. El cierre normal `1000`, los rechazos de protocolo o autenticación, códigos privados `4xxx` y demás cierres dejan el cliente inactivo. Un mensaje de protocolo inválido cierra con `4002` y no reintenta. `stop()` y `dispose()` también detienen la reconexión. Un `error` del transporte espera su código de cierre; si no llega durante `closeTimeoutMs`, se trata como un fallo de conexión que puede reintentarse. Los fallos iniciales de handshake, URL o conexión también pueden reintentarse; la promesa original de `connect()` permanece pendiente hasta alcanzar disponibilidad o agotar los intentos.

Los detalles del cierre también dependen del transporte del servidor. El adaptador RSGI de Orionis no expone códigos ni motivos personalizados de cierre WebSocket saliente, por lo que un cierre iniciado por el servidor puede verse como código normal `1000`. El cliente no puede deducir un reinicio ni un rechazo de política a partir de ese código y no lo reintenta automáticamente. ASGI admite detalles de cierre explícitos. Tras un cierre no recuperable que deja el cliente inactivo, la aplicación puede decidir ejecutar otro `connect()` según su propio estado y política de acceso.

WebSocket del navegador no permite añadir encabezados HTTP arbitrarios de autenticación. Los subprotocolos no constituyen autorización. Configura el acceso en el handshake y las operaciones del Hub.

## Errores y límites JSON

Exports con nombre: `RealtimeError`, `ConnectionError`, `ProtocolError`, `RemoteError`, `TimeoutError`, `AbortError` y `LimitError`.

- `RemoteError` conserva `code` y `message` del servidor.
- Los códigos locales de timeout de operación, cancelación, protocolo y límites son `timeout`, `aborted`, `protocol_error` y `limit_exceeded`. Un plazo agotado en la cola de envío utiliza `send_timeout`; lecturas concurrentes de un stream utilizan `concurrent_next`.
- Los errores de conexión incluyen `connection_lost`, `not_connected`, `connect_failed`, `connect_timeout`, `heartbeat_timeout`, `send_failed`, `stopped` y `disposed`.
- Los errores ordinarios de handlers se normalizan a `callback_error`; se conservan los códigos de errores que ya son `RealtimeError`. Los fallos de observadores de errores se aíslan.

Sólo se admiten datos JSON simples. Se rechazan ciclos, getters, números no finitos, `undefined`, `bigint`, `Date` y objetos no simples, en lugar de convertirlos silenciosamente. Claves JSON como `__proto__` y `constructor` permanecen como datos inertes. Los mensajes entrantes se limitan y validan; un frame de protocolo inválido termina la conexión y no se interpreta como un resultado satisfactorio.

Los números JSON utilizan `Number` de JavaScript. Los identificadores enteros fuera del rango seguro `±Number.MAX_SAFE_INTEGER` pueden perder precisión al analizarse; represéntalos como strings en el protocolo de tu aplicación. `BigInt` no se serializa automáticamente.

Las declaraciones describen resultados JSON; no sustituyen la validación del servidor ni demuestran que un resultado remoto tenga la estructura de tu aplicación.

## Desarrollo e integridad

Desde el directorio del proyecto, instala las dependencias de desarrollo y ejecuta las comprobaciones:

```bash
npm ci
npm run check
npm pack --dry-run
```

Las comprobaciones cubren sintaxis JavaScript, comportamiento del cliente, consumidores TypeScript, el archivo de npm e integridad SHA-256. Las pruebas y dependencias de desarrollo se excluyen del archivo publicado.

`integrity.json` registra nombre, versión, tamaño en bytes y SHA-256 de cada archivo publicado excepto él mismo. Después de modificar un archivo publicado, ejecuta `npm run integrity` para regenerarlo; `npm run integrity:check` lo verifica sin escribir. Estas comprobaciones detectan cambios, no la autenticidad de un manifiesto descargado por separado.

## Publicación

Utiliza la cuenta npm que será propietaria de `orionis-realtime-client`. La publicación directa requiere autenticación de dos factores o una credencial de publicación admitida por npm; nunca guardes credenciales en este proyecto.

```bash
npm login --registry=https://registry.npmjs.org/
npm whoami --registry=https://registry.npmjs.org/
npm publish --dry-run
npm publish --access public
```

El hook `prepack` regenera la integridad y ejecuta todas las comprobaciones antes de empaquetar o publicar. `--dry-run` no publica. La primera versión es `0.1.0`; cada versión posterior necesita un número nuevo que no se haya publicado. npm comprueba la disponibilidad del nombre y los permisos de la cuenta al publicar.

## Licencia

MIT. El copyright original y el aviso completo se conservan en [LICENSE](./LICENSE) y en el módulo independiente.
