<?php

namespace Hail\Database\Event;


class Collector
{
    /**
     * @var Event[]
     */
    protected static $items = [];

    public static function add(Event $item): void
    {
        static::$items[] = $item;
    }

    /**
     * @return float ms
     */
    public static function getTotalElapsedTime(): float
    {
        $elapsed = 0;
        foreach (static::$items as $item) {
            $elapsed += $item->getElapsedTime();
        }

        return $elapsed;
    }

    /**
     * @return array
     */
    public static function getAggregations(): array
    {
        $elapsed = [];
        foreach (static::$items as $item) {
            $storage = $item->getStorageType();
            $db = $item->getDatabaseName();
            $key = "$storage|$db";
            if (!isset($elapsed[$key])) {
                $elapsed[$key] = [
                    'storageType' => $storage,
                    'databaseName' => $db,
                    'elapsed' => 0,
                    'count' => 0,
                ];
            }
            $elapsed[$key]['elapsed'] += $item->getElapsedTime();
            $elapsed[$key]['count'] += 1;
        }
        \usort($elapsed, static function ($a, $b) {
            return $a['elapsed'] < $b['elapsed'];
        });

        return $elapsed;
    }

    /**
     * @return Event[] ordered by time called asc
     */
    public static function get(): array
    {
        return static::$items;
    }


    /**
     * @return int
     */
    public static function count(): int
    {
        return \count(static::$items);
    }

    /**
     * @return array [float $min, float $max]
     */
    public static function getTimeExtremes(): array
    {
        $times = [];
        foreach (static::$items as $item) {
            $times[] = $item->getElapsedTime();
        }

        return [\min($times), \max($times)];
    }
}
