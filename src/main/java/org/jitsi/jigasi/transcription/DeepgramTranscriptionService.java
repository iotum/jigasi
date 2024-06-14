/*
 * Jigasi, the JItsi GAteway to SIP.
 */
package org.jitsi.jigasi.transcription;

import org.eclipse.jetty.client.*;
import org.eclipse.jetty.client.api.*;
import org.eclipse.jetty.client.util.BytesRequestContent;
import org.eclipse.jetty.http.*;
import org.eclipse.jetty.websocket.api.*;
import org.eclipse.jetty.websocket.api.annotations.*;
import org.eclipse.jetty.websocket.client.*;
import org.json.*;
import org.jitsi.jigasi.*;
import org.jitsi.utils.concurrent.*;
import org.jitsi.utils.logging.*;

import javax.media.format.*;
import java.net.*;
import java.nio.*;
import java.time.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.*;
import java.util.regex.Pattern;

/**
 * Implements a TranscriptionService which uses Deepgram websocket transcription service.
 * <p>
 * See https://deepgram.com/
 *
 * @author Jerry Hu
 */
public class DeepgramTranscriptionService
        extends AbstractTranscriptionService
{

    /**
     * BCP-47 (see https://www.rfc-editor.org/rfc/bcp/bcp47.txt)
     * language tags of the languages supported by Deepgram API
     * (See https://developers.deepgram.com/docs/models-languages-overview)
     */
    public final static String[] SUPPORTED_LANGUAGE_TAGS = new String[]
        {
            "bg", // Bulgarian
            "ca", // Catalan
            "zh", "zh-CN", "zh-Hans", // Chinese (Mandarin, Simplified)
            "zh-TW", "zh-Hant", // Chinese (Mandarin, Traditional)
            "cs", // Czech
            "da", "da-DK", // Danish
            "nl", // Dutch
            "en", "en-US", "en-AU", "en-GB", "en-NZ", "en-IN", // English
            "et", // Estonian
            "fi", // Finnish
            "nl-BE", // Flemish
            "fr", "fr-CA", // French
            "de", // German
            "de-CH", // German (Switzerland)
            "el", // Greek
            "hi", "hi-Latn", // Hindi
            "hu", // Hungarian
            "id", // Indonesian
            "it", // Italian
            "ja", // Japanese
            "ko", "ko-KR", // Korean
            "lv", // Latvian
            "lt", // Lithuanian
            "ms", // Malay
            "no", // Norwegian
            "pl", // Polish
            "pt", "pt-BR", // Portuguese
            "ro", // Romanian
            "ru", // Russian
            "sk", // Slovak
            "es", "es-419", // Spanish
            "sv", "sv-SE", // Swedish
            "th", "th-TH", // Thai
            "tr", // Turkish
            "uk", // Ukrainian
            "vi", // Vietnamese
        };

    /**
     * The logger for this class
     */
    private final static Logger logger
            = Logger.getLogger(DeepgramTranscriptionService.class);

    /**
     * The config key of the api endpoint to the speech-to-text service.
     */
    public final static String API_ENDPOINT
            = "org.jitsi.jigasi.transcription.deepgram.endpoint";

    /**
     * The config key of the api features to the speech-to-text service.
     */
    public final static String API_FEATURES
            = "org.jitsi.jigasi.transcription.deepgram.features";

    /**
     * The config key of the authentication token to the speech-to-text service.
     */
    public final static String API_TOKEN
            = "org.jitsi.jigasi.transcription.deepgram.api_token";

    public final static String DEFAULT_ENDPOINT
            = "api.deepgram.com/v1/listen";

    public final static String DEFAULT_FEATURES
            = "punctuate=true&interim_results=true";

    public final static Pattern DEFAULT_MODEL_BASE
            = Pattern.compile("^(zh-CN|zh-TW|fr-CA|hi-Latn|id|ru|tr|uk)$");

    public final static Pattern DEFAULT_MODEL_NOVA
            = Pattern.compile("^(en-AU|en-GB|en-IN|en-NZ)$");

    private final static String KEEPALIVE_MESSAGE = "{\"type\" : \"KeepAlive\"}";

    private final static String EOF_MESSAGE = "{\"type\" : \"CloseStream\"}";

    /**
     * The executor used to perform periodic health checks.
     */
    private static final RecurringRunnableExecutor EXECUTOR
        = new RecurringRunnableExecutor(DeepgramTranscriptionService.class.getName());


    /**
     * The endpoint to the speech-to-text service.
     */
    private String apiEndpoint;

    /**
     * The client with Authorization to the speech-to-text service.
     */
    private ClientUpgradeRequest clientUpgradeRequest;

    /**
     * The time connection to the service is ready.
     */
    private Instant initTimestamp;

    /**
     * Check whether the given string contains a supported language tag
     *
     * @param tag the language tag
     * @throws UnsupportedOperationException when the google cloud API does not
     * support the given language
     */
    private static void validateLanguageTag(String tag)
        throws UnsupportedOperationException
    {
        for (String supportedTag : SUPPORTED_LANGUAGE_TAGS)
        {
            if (supportedTag.equals(tag))
            {
                return;
            }
        }
        throw new UnsupportedOperationException(tag + " is not a language " +
                                                "supported by the Deepgram API");
    }

    /**
     * Create a TranscriptionService which will send audio to the Deepgram service
     * platform to get a transcription
     */
    public DeepgramTranscriptionService()
    {
        apiEndpoint = JigasiBundleActivator.getConfigurationService().getString(API_ENDPOINT, DEFAULT_ENDPOINT)
                + "?" + JigasiBundleActivator.getConfigurationService().getString(API_FEATURES, DEFAULT_FEATURES);

        String apiToken = JigasiBundleActivator.getConfigurationService().getString(API_TOKEN);
        if (apiToken != null && apiToken.length() > 0)
        {
            clientUpgradeRequest = new ClientUpgradeRequest();
            clientUpgradeRequest.setHeader("Authorization", "Token " + apiToken);
        }
    }

    /**
     * No configuration required yet
     */
    public boolean isConfiguredProperly()
    {
        return clientUpgradeRequest != null;
    }

    /**
     * Language routing is handled by Deepgram directly
     */
    public boolean supportsLanguageRouting()
    {
        return false;
    }

    /**
     * Sends audio as an array of bytes to Deepgram service
     *
     * @param request        the TranscriptionRequest which holds the audio to be sent
     * @param resultConsumer a Consumer which will handle the
     *                       TranscriptionResult
     */
    @Override
    public void sendSingleRequest(final TranscriptionRequest request,
                                  final Consumer<TranscriptionResult> resultConsumer)
    {
        logger.warn("The Deepgram transcription service does not support single requests.");
    }

    @Override
    public StreamingRecognitionSession initStreamingSession(Participant participant)
        throws UnsupportedOperationException
    {
        logger.info("InitStreamingSession for participant " + participant.getDebugName()
            + " (" + participant.getSourceLanguage() + "/" + participant.getTranslationLanguage() + ")");
        try
        {
            DeepgramWebsocketStreamingSession streamingSession = new DeepgramWebsocketStreamingSession(
                    participant.getDebugName());
            streamingSession.transcriptionTag = participant.getTranslationLanguage();
            if (streamingSession.transcriptionTag == null)
            {
                streamingSession.transcriptionTag = participant.getSourceLanguage();
            }
            if (streamingSession.transcriptionTag == null)
            {
                streamingSession.transcriptionTag = "en";
            }
            return streamingSession;
        }
        catch (Exception e)
        {
            throw new UnsupportedOperationException("Failed to create streaming session", e);
        }
    }

    @Override
    public boolean supportsFragmentTranscription()
    {
        return true;
    }

    @Override
    public boolean supportsStreamRecognition()
    {
        return true;
    }

    /**
     * A Transcription session for transcribing streams, handles
     * the lifecycle of websocket
     */
    @WebSocket
    public class DeepgramWebsocketStreamingSession
        implements StreamingRecognitionSession
    {
        private Session session;
        /* The name of the participant */
        private final String debugName;
        /* The sample rate of the audio stream we collect from the first request */
        private int sampleRate = -1;
        /* Last returned result so we do not return the same string twice */
        private String lastResult = "";
        /* Transcription language requested by the user who requested the transcription */
        private String transcriptionTag = "en-US";

        /**
         * KeepAlive (see https://developers.deepgram.com/docs/keep-alive)
         * The Keep-Alive message will keep the streaming connection open for an additional 12 seconds.
         */
        private DeepgramKeepAliveWebsocket keepAliveChecker
            = new DeepgramKeepAliveWebsocket(3_000);

        /**
         * List of TranscriptionListeners which will be notified when a
         * result comes in
         */
        private final List<TranscriptionListener> listeners = new ArrayList<>();

        /**
         *  Latest assigned UUID to a transcription result.
         *  A new one has to be generated whenever a definitive result is received.
         */
        private UUID uuid = UUID.randomUUID();

        DeepgramWebsocketStreamingSession(String debugName)
        {
            this.debugName = debugName;
        }

        @OnWebSocketClose
        public void onClose(int statusCode, String reason)
        {
            EXECUTOR.deRegisterRecurringRunnable(keepAliveChecker);

            this.session = null;

            logger.warn("Connection closed for participant " + debugName);
        }

        @OnWebSocketConnect
        public void onConnect(Session session)
        {
            logger.warn("Connection connected for participant " + debugName);

            this.session = session;

            EXECUTOR.registerRecurringRunnable(keepAliveChecker);
        }

        @OnWebSocketMessage
        public void onMessage(String msg)
        {
            if (logger.isDebugEnabled())
                logger.debug(debugName + ": received response: " + msg);

            JSONObject obj = new JSONObject(msg);
            if (!obj.has("channel"))
            {
                return;
            }

            long startMicro = (long)(obj.getFloat("start") * 1_000_000);
            boolean partial = !obj.getBoolean("is_final");
            JSONObject alternative = obj.getJSONObject("channel")
                    .getJSONArray("alternatives")
                    .getJSONObject(0);
            String result = alternative.getString("transcript");

            if (!result.isEmpty() && (!partial || !result.equals(lastResult)))
            {
                lastResult = result;
                for (TranscriptionListener l : listeners)
                {
                    l.notify(new TranscriptionResult(
                            null,
                            uuid,
                            initTimestamp.plus(startMicro, ChronoUnit.MICROS),
                            partial,
                            transcriptionTag,
                            alternative.getFloat("confidence"),
                            new TranscriptionAlternative(
                                result,
                                alternative.getFloat("confidence")
                            )));
                }
            }
            else
            {
                if (logger.isDebugEnabled())
                {
                    logger.debug(
                            debugName + ": dropping result without any change to"
                                    + " the stable part");
                }
            }

            if (!partial)
            {
                this.uuid = UUID.randomUUID();
            }
        }

        @OnWebSocketError
        public void onError(Throwable cause)
        {
            logger.error("Error while streaming audio data to transcription service" , cause);
        }

        public void sendRequest(TranscriptionRequest request)
        {
            logger.trace("Send request for participant " + debugName);

            if (sampleRate < 0)
            {
                connect(request);
            }

            try
            {
                ByteBuffer audioBuffer = ByteBuffer.wrap(request.getAudio());
                session.getRemote().sendBytes(audioBuffer);
            }
            catch (Exception e)
            {
                logger.error("Error to send websocket request for participant " + debugName, e);
            }
        }

        public void addTranscriptionListener(TranscriptionListener listener)
        {
            listeners.add(listener);
        }

        public void end()
        {
            try
            {
                if (session != null)
                    session.getRemote().sendString(EOF_MESSAGE);
            }
            catch (Exception e)
            {
                logger.error("Error to finalize websocket connection for participant " + debugName, e);
            }
        }

        public boolean ended()
        {
            return session == null;
        }

        void connect(TranscriptionRequest request)
        {
            synchronized (this)
            {
                if (sampleRate > 0)
                    return;
                sampleRate = Double.valueOf(request.getFormat().getSampleRate()).intValue();
            }

            try
            {
                String languageTag = request.getLocale().toLanguageTag();
                validateLanguageTag(languageTag);

                ArrayList<String> parts = new ArrayList<>();
                parts.add(apiEndpoint);
                parts.add("language=" + languageTag);
                parts.add("encoding=" + "linear16");
                parts.add("sample_rate=" + Integer.toString(sampleRate));
                // https://developers.deepgram.com/docs/models-languages-overview
                // parts.add("model=nova-2"); // not available to us yet
                if (DEFAULT_MODEL_NOVA.matcher(languageTag).matches())
                {
                    parts.add("model=nova");
                }
                else if (DEFAULT_MODEL_BASE.matcher(languageTag).matches())
                {
                    parts.add("model=base");
                }
                else
                {
                    parts.add("model=enhanced");
                }
                String url = "wss://" + String.join("&", parts);

                logger.info("Connecting to " + url + " for participant " + debugName);
                WebSocketClient ws = new WebSocketClient();
                ws.start();

                session = ws.connect(this, new URI(url), clientUpgradeRequest).get();
                initTimestamp = Instant.now();
                logger.info("Connected at " + initTimestamp + " for participant " + debugName);
            }
            catch (Exception e)
            {
                logger.error("Error to create websocket connection for participant " + debugName, e);
            }
        }

        private class DeepgramKeepAliveWebsocket
            extends PeriodicRunnable
        {
            public DeepgramKeepAliveWebsocket(long period)
            {
                super(period);
            }

            @Override
            public void run()
            {
                super.run();
                try
                {
                    session.getRemote().sendString(KEEPALIVE_MESSAGE);
                }
                catch (Exception e)
                {
                    logger.error("Error to send keepalive for participant " + debugName, e);
                }
            }
        }
    }

}
