import SuperJSON from 'superjson';

let isBufferRegistered = false;

function ensureBufferSupport() {
  if (isBufferRegistered) {
    return;
  }

  SuperJSON.registerCustom<Buffer, string>(
    {
      isApplicable: (value): value is Buffer => Buffer.isBuffer(value),
      serialize: (value) => value.toString('base64'),
      deserialize: (value) => Buffer.from(value, 'base64'),
    },
    'node-buffer-base64',
  );

  isBufferRegistered = true;
}

ensureBufferSupport();

export { SuperJSON };
