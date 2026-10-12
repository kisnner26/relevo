import {
  GraphQLBoolean,
  GraphQLEnumType,
  GraphQLFloat,
  GraphQLInt,
  GraphQLList,
  GraphQLNonNull,
  GraphQLObjectType,
  GraphQLScalarType,
  GraphQLSchema,
  GraphQLString,
  Kind,
} from 'graphql';
import { jobTypes } from './jobs.js';

function literal(ast) {
  switch (ast.kind) {
    case Kind.OBJECT:
      return Object.fromEntries(ast.fields.map((f) => [f.name.value, literal(f.value)]));
    case Kind.LIST:
      return ast.values.map(literal);
    case Kind.INT:
    case Kind.FLOAT:
      return Number(ast.value);
    case Kind.NULL:
      return null;
    default:
      return ast.value;
  }
}

const JSONValue = new GraphQLScalarType({
  name: 'JSON',
  description: 'Any JSON value',
  serialize: (v) => v,
  parseValue: (v) => v,
  parseLiteral: literal,
});

const JobStatus = new GraphQLEnumType({
  name: 'JobStatus',
  values: Object.fromEntries(['queued', 'running', 'done', 'failed', 'cancelled'].map((s) => [s, { value: s }])),
});

const Priority = new GraphQLEnumType({
  name: 'Priority',
  values: { high: { value: 'high' }, normal: { value: 'normal' }, low: { value: 'low' } },
});

const JobType = new GraphQLEnumType({
  name: 'JobType',
  values: Object.fromEntries(jobTypes.map((t) => [t, { value: t }])),
});

const JobError = new GraphQLObjectType({
  name: 'JobError',
  fields: { code: { type: GraphQLString }, message: { type: GraphQLString } },
});

const Job = new GraphQLObjectType({
  name: 'Job',
  fields: {
    id: { type: new GraphQLNonNull(GraphQLString) },
    type: { type: new GraphQLNonNull(JobType) },
    status: { type: new GraphQLNonNull(JobStatus) },
    priority: { type: Priority },
    progress: { type: GraphQLFloat },
    cached: { type: GraphQLBoolean },
    shared: { type: GraphQLBoolean },
    params: { type: JSONValue },
    result: { type: JSONValue },
    error: { type: JobError },
    instance: { type: GraphQLString },
    createdAt: { type: GraphQLFloat },
    startedAt: { type: GraphQLFloat },
    finishedAt: { type: GraphQLFloat },
  },
});

const Stats = new GraphQLObjectType({
  name: 'Stats',
  fields: {
    instance: { type: GraphQLString },
    pool: { type: JSONValue },
    cache: { type: JSONValue },
    jobs: { type: JSONValue },
    eventLoop: { type: JSONValue },
  },
});

export function buildSchema({ service, metrics }) {
  const Query = new GraphQLObjectType({
    name: 'Query',
    fields: {
      job: {
        type: Job,
        args: { id: { type: new GraphQLNonNull(GraphQLString) } },
        resolve: (_, { id }) => service.get(id),
      },
      jobs: {
        type: new GraphQLList(new GraphQLNonNull(Job)),
        args: { status: { type: JobStatus }, limit: { type: GraphQLInt, defaultValue: 50 } },
        resolve: (_, args) => service.list(args),
      },
      stats: { type: Stats, resolve: () => metrics.snapshot() },
    },
  });

  const Mutation = new GraphQLObjectType({
    name: 'Mutation',
    fields: {
      enqueue: {
        type: new GraphQLNonNull(Job),
        args: {
          type: { type: new GraphQLNonNull(JobType) },
          params: { type: JSONValue, defaultValue: {} },
          priority: { type: Priority, defaultValue: 'normal' },
        },
        resolve: (_, args) => service.enqueue(args),
      },
      cancel: {
        type: new GraphQLNonNull(Job),
        args: { id: { type: new GraphQLNonNull(GraphQLString) } },
        resolve: (_, { id }) => service.cancel(id),
      },
    },
  });

  return new GraphQLSchema({ query: Query, mutation: Mutation });
}
