# dal
Data Access Layer

`dal` is the easiest way to enable programmatic access to your dbt project.

It works by generating a GraphQL API from your dbt config files. GraphQL is
widely supported and this makes it incredibly simple for other engineers to
work with the data you have lovingly prepared.

## Demo

https://www.loom.com/share/68493cd7620045fd9f538066e245f1ef

## Data warehouse support

It only supports snowflake at the moment.

## How does it work?

All you have to do is include a little bit of metadata to tell `dal` which models you would like to expose. You can then start the server from inside your dbt project, and that's it.

To install `dal`, simply run:

```
brew tap supasheet/dal
brew install dal
```

Then you can cd into your dbt project, add some dal metadata and run:

```
dal serve
```

That's it!

Check out the demo video for a bit more information on how to configure dal
metadata. It's very simple, all you need to do is add the following to any
models you want to expose:

```
meta:
  dal:
    expose: true
```

